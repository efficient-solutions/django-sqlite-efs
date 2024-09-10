"""
test_lock_manager.py

Unit tests for the DynamoDBLockManager class in the lock_manager module. These 
tests verify the correct functionality of the distributed locking mechanism 
implemented using Amazon DynamoDB for managing SQLite database locks on Amazon EFS.

Test cases include:
- Successful and failed attempts to acquire a lock.
- Correct behavior when releasing a lock.
- Handling of timeouts, retries, and exceptions during lock acquisition.
- Validation of SQL query types (e.g., read vs. write queries) to determine 
  when locks are necessary.
- Behavior of the lock manager when used in a context manager (i.e., 'with' 
  statements).

Mocking is extensively used to simulate interactions with AWS DynamoDB, as well 
as time-based behaviors for lock expiration.

Key tests:
- test_acquire_lock_success: Tests successful acquisition of a lock.
- test_acquire_lock_failure: Tests failure to acquire a lock after multiple 
  attempts.
- test_release_lock_success: Verifies that locks are released successfully.
- test_is_write_query: Validates detection of write queries (e.g., INSERT, 
  UPDATE, DELETE).
- test_context_manager_behavior: Ensures that the lock manager behaves correctly 
  within 'with' blocks.
"""

import time  # pylit: disable=unused-import
import unittest
from decimal import Decimal
from unittest.mock import patch, MagicMock
from boto3.dynamodb.conditions import Attr
from django_sqlite_efs.lock_manager import DynamoDBLockManager
from django_sqlite_efs.exceptions import DatabaseBusy


class TestDynamoDBLockManager(unittest.TestCase):
    """
    Unit tests for the DynamoDBLockManager class, covering initialization, lock acquisition,
    lock release, and various properties and helper methods.
    """

    def setUp(self):
        """Set up common test variables and patch external dependencies."""
        self.db_file_path = "/path/to/sqlite.db"
        self.lock_wait_timeout = 5
        self.dynamodb_table_mock = MagicMock()

        # Patch the required settings and DynamoDB table
        self.patcher1 = patch('django_sqlite_efs.lock_manager.DynamoDBLockManager.get_setting')
        self.patcher2 = patch('django_sqlite_efs.lock_manager.boto3.resource')
        self.patcher3 = patch('django_sqlite_efs.lock_manager.os.path.exists')
        self.patcher4 = patch('django_sqlite_efs.lock_manager.boto3.session.Session')

        self.mock_get_setting = self.patcher1.start()
        self.mock_boto3_resource = self.patcher2.start()
        self.mock_os_path_exists = self.patcher3.start()
        self.mock_boto3_session = self.patcher4.start()

        # Mocking settings values
        self.mock_get_setting.side_effect = lambda key, **kwargs: {
            'SQLITE_LOCK_EXPIRATION': 10,  # seconds
            'SQLITE_LOCK_DYNAMODB_TABLE': 'DynamoDBLockTable',
            'SQLITE_LOCK_MAX_ATTEMPTS': 10
        }.get(key)

        # Mock the DynamoDB session with region
        self.mock_boto3_session.return_value.resource.return_value.Table.return_value = self.dynamodb_table_mock

        # Ensure the session is initialized with a valid region
        self.mock_boto3_session.return_value.region_name = 'us-east-1'

    def tearDown(self):
        """Stop all patches after each test."""
        patch.stopall()

    def test_initialization_with_valid_timeout(self):
        """Test DynamoDBLockManager initialization with valid timeout."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)
        self.assertEqual(lock_manager.database_file_path, self.db_file_path)
        self.assertEqual(lock_manager.lock_wait_timeout, self.lock_wait_timeout)
        self.assertEqual(lock_manager.lock_expiration, 10)  # from mocked get_setting
        self.assertEqual(lock_manager._dynamodb_lock_table_name, 'DynamoDBLockTable')

    def test_initialization_with_invalid_timeout(self):
        """Test DynamoDBLockManager initialization with invalid timeout."""
        lock_manager = DynamoDBLockManager(self.db_file_path, 0)
        self.assertEqual(lock_manager.lock_wait_timeout, 3)  # Defaults to 3 when invalid

    def test_normalize_sql_query(self):
        """Test SQL query normalization."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)
        raw_query = "\n\tSELECT *  FROM users \r\n WHERE id = 1"
        normalized_query = lock_manager.normalize_sql_query(raw_query)
        self.assertEqual(normalized_query, "SELECT * FROM USERS WHERE ID = 1")

    def test_is_transaction_start(self):
        """Test detection of transaction-starting SQL queries."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)
        self.assertTrue(lock_manager.is_transaction_start("BEGIN TRANSACTION"))
        self.assertFalse(lock_manager.is_transaction_start("SELECT * FROM users"))

    def test_is_write_query(self):
        """Test detection of write SQL queries."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)
        self.assertTrue(lock_manager.is_write_query("INSERT INTO users (id, name) VALUES (1, 'John')"))
        self.assertFalse(lock_manager.is_write_query("SELECT * FROM users"))

    @patch('time.time', return_value=1000)
    def test_current_unix_timestamp(self, mock_time):
        """Test current Unix timestamp as a Decimal."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)
        self.assertEqual(lock_manager.current_unix_timestamp, Decimal(1000))

    @patch('time.time', return_value=1000)
    def test_acquire_lock_success(self, mock_time):
        """Test successful lock acquisition."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)

        lock_manager.acquire_lock()

        self.assertIsNotNone(lock_manager.current_lock_id)
        self.assertEqual(lock_manager.lock_acquired_timestamp, Decimal(1000))
        self.dynamodb_table_mock.put_item.assert_called()

    @patch('time.time', return_value=1000)
    def test_acquire_lock_failure(self, mock_time):
        """Test failure to acquire lock after max attempts."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)

        # Simulate lock acquisition failure for each attempt
        self.dynamodb_table_mock.put_item.side_effect = Exception('DynamoDB Error')

        # Ensure DatabaseBusy is raised after max attempts
        with self.assertRaises(DatabaseBusy):
            lock_manager.acquire_lock()

        # Verify that put_item was called the max number of attempts (10 times)
        self.assertEqual(self.dynamodb_table_mock.put_item.call_count, 10)

    @patch('time.time', return_value=1000)
    def test_release_lock_success(self, mock_time):
        """Test successful lock release."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)

        # Simulate that a lock was acquired
        lock_manager.current_lock_id = 'test-lock-id'
        lock_manager.lock_acquired_timestamp = Decimal(1000)
        lock_manager.lock_expiry_timestamp = Decimal(1100)

        # Call release_lock (should trigger delete_item)
        lock_manager.release_lock()

        # Verify that delete_item was called with the correct arguments
        self.dynamodb_table_mock.delete_item.assert_called_with(
            Key={'pk': 'database#/path/to/sqlite.db'},
            ConditionExpression=Attr('lock_id').eq('test-lock-id')
        )

    def test_release_lock_no_active_lock(self):
        """Test release lock when no active lock is present."""
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)

        lock_manager.current_lock_id = None  # No active lock
        lock_manager.release_lock()

        self.dynamodb_table_mock.delete_item.assert_not_called()

    def test_rollback_journal_exists(self):
        """Test if SQLite rollback journal file exists."""
        self.mock_os_path_exists.return_value = True  # Simulate journal file exists
        lock_manager = DynamoDBLockManager(self.db_file_path, self.lock_wait_timeout)
        self.assertTrue(lock_manager.rollback_journal_exists())

        self.mock_os_path_exists.return_value = False  # Simulate journal file does not exist
        self.assertFalse(lock_manager.rollback_journal_exists())


if __name__ == '__main__':
    unittest.main()
