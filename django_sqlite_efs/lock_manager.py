"""
lock_manager.py: Provides a distributed locking mechanism for SQLite databases
on Amazon EFS using DynamoDB.
"""

import os
import time
import uuid
import logging
from typing import Any
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.table import TableResource

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from django.conf import settings
from .exceptions import DatabaseBusy

logger = logging.getLogger(__name__)


class DynamoDBLockManager():  # pylint: disable=too-many-instance-attributes
    """
    A manager for database locking using Amazon DynamoDB.

    This class facilitates the acquisition and release of locks on SQLite databases 
    hosted on Amazon EFS using DynamoDB as the underlying distributed locking system. 
    It ensures safe concurrent access to the database by controlling write operations.
    """

    def __init__(self, database_file_path: str, lock_wait_timeout: int | None) -> None:
        """
        Initialize the DynamoDBLockManager instance.

        Args:
            database_file_path (str): The file path of the SQLite database.
            lock_wait_timeout (int | None): The maximum time, in seconds, to wait for a lock.
                                            If None or invalid, defaults to 3 seconds.
        """
        self.database_file_path: str = database_file_path
        if lock_wait_timeout is not None and lock_wait_timeout >= 1:
            self.lock_wait_timeout = lock_wait_timeout
        else:
            self.lock_wait_timeout = 3
        self.max_lock_attempts = int(
            self.get_setting('SQLITE_LOCK_MAX_ATTEMPTS', default=10, required=False)
        )
        self.lock_expiration: int = int(self.get_setting('SQLITE_LOCK_EXPIRATION'))
        self._dynamodb_lock_table_name: str = self.get_setting('SQLITE_LOCK_DYNAMODB_TABLE')
        self.current_lock_id: str | None = None
        self.lock_acquired_timestamp: Decimal | None = None
        self.lock_expiry_timestamp: Decimal | None = None
        self._dynamodb_lock_table: TableResource | None = None
        self.current_sql_query: str | None = None
        self.is_transaction: bool = False

    def __enter__(self):
        """
        Enter the context manager to acquire a lock if needed.

        Called when the DynamoDBLockManager is used with a 'with' statement. It decides 
        whether to acquire a lock based on the type of SQL query being executed.

        Returns:
            self: The DynamoDBLockManager instance.
        """
        if not self.current_sql_query:
            self.acquire_lock()
        elif self.is_transaction_start(self.current_sql_query):
            self.is_transaction = True
            self.acquire_lock()
        elif self.is_write_query(self.current_sql_query):
            self.acquire_lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context manager and release the lock if necessary.

        This method releases the lock when exiting the 'with' block, unless the current 
        operation is part of a transaction, in which case the lock remains active.

        Args:
            exc_type: The type of exception raised (if any).
            exc_value: The exception instance (if any).
            traceback: A traceback object providing the stack trace (if applicable).
        """
        if not self.is_transaction:
            self.release_lock()
        self.current_sql_query = None

    def get_setting(self, key: str, default: Any = None, required: bool = True) -> Any:
        """
        Retrieve a configuration setting from Django settings or environment variables.

        This function tries to retrieve the specified setting from Django's settings module. 
        If the setting is not found in Django settings, it looks for the same setting 
        in the environment variables. If neither source provides a value, it returns a default 
        or raises an exception if the setting is marked as required.

        Args:
            key (str): The name of the setting to retrieve.
            default (Any, optional): A default value to return if the setting is not found. 
                                    Defaults to None.
            required (bool, optional): Whether the setting is mandatory. If True and the setting 
                                    is not found, raises an exception. Defaults to True.

        Raises:
            ImproperlyConfigured: Raised if the setting is required but not found in either
                                Django settings or environment variables.

        Returns:
            Any: The value of the requested setting, or the default value if not found.
        """
        # Attempt to get the setting from Django's settings or environment variables.
        value = getattr(settings, key, os.environ.get(key, default))
        # Raise an error if the setting is required but not set.
        if required and value is None:
            raise ImproperlyConfigured(f"{key} or environment variable {key} is required but not set.")
        return value

    def rollback_journal_exists(self):
        """
        Check if the SQLite rollback journal file exists.

        SQLite uses a rollback journal during transactions. This method checks whether 
        the journal file exists, indicating that a transaction is in progress.

        Returns:
            bool: True if the rollback journal exists, False otherwise.
        """
        journal_file = f"{self.database_file_path}-journal"
        return os.path.exists(journal_file)

    def normalize_sql_query(self, query: str) -> str:
        """
        Normalize an SQL query by removing excess whitespace and converting to uppercase.

        Args:
            query (str): The SQL query to normalize.

        Returns:
            str: The normalized SQL query, with unnecessary whitespace removed and 
                 all characters converted to uppercase.
        """
        # Remove tabs, newlines, and carriage returns, then join words and convert to uppercase
        return " ".join(
            query.replace("\t", "").replace("\n", "").replace("\r", "").split()
        ).upper()

    def is_transaction_start(self, query: str) -> bool:
        """
        Determine whether the given query initiates a transaction.

        Args:
            query (str): The SQL query to check.

        Returns:
            bool: True if the query starts a transaction, False otherwise.
        """
        return self.normalize_sql_query(query).startswith("BEGIN")

    def is_write_query(self, query: str) -> bool:
        """
        Check if the given query is a write operation.

        Args:
            query (str): The SQL query to check.

        Returns:
            bool: True if the query performs a write operation (e.g., INSERT, UPDATE, DELETE), 
                  False otherwise.
        """
        query = self.normalize_sql_query(query)
        # SELECT and EXPLAIN queries are considered read-only
        return not query.startswith("SELECT") and not query.startswith("EXPLAIN")

    def set_query_for_context(self, query: str):
        """
        Set the current SQL query for context management.

        This method stores the SQL query that will be used in the context of the lock manager.

        Args:
            query (str): The SQL query to set.

        Returns:
            self: The DynamoDBLockManager instance.
        """
        self.current_sql_query = self.normalize_sql_query(query)
        return self

    @property
    def current_unix_timestamp(self) -> Decimal:
        """
        Get the current Unix timestamp as a Decimal.

        Returns:
            Decimal: The current Unix timestamp.
        """
        return Decimal(time.time())

    @property
    def is_lock_active(self) -> bool:
        """
        Check if there is an active lock for the current database in this request.

        Returns:
            bool: True if a lock is active and not expired, False otherwise.
        """
        if self.current_lock_id is not None and self.is_lock_expired is False:
            return True
        return False

    @property
    def is_lock_expired(self) -> bool:
        """
        Check if the current lock has expired.

        Returns:
            bool: True if the lock has expired or does not exist, False otherwise.
        """
        if self.lock_expiry_timestamp is None or \
            self.lock_expiry_timestamp <= self.current_unix_timestamp:
            return True
        return False

    @property
    def dynamodb_lock_table(self):
        """
        Lazily initialize and return the DynamoDB table resource for locking.

        Configures and initializes the boto3 DynamoDB table resource, caching it for 
        future use to avoid repeated initialization.

        Returns:
            TableResource: The DynamoDB table resource.
        """
        if self._dynamodb_lock_table:
            return self._dynamodb_lock_table
        # Configure boto3 for quick timeouts and minimal retries
        boto_config = Config(
            retries={
                'total_max_attempts': 2,
                'mode': 'standard'
            },
            connect_timeout=1,
            read_timeout=1
        )
        session = boto3.session.Session()
        resource = session.resource(
            service_name="dynamodb",
            config=boto_config
        )
        self._dynamodb_lock_table = resource.Table(self._dynamodb_lock_table_name)
        return self._dynamodb_lock_table

    @property
    def dynamodb_primary_key(self) -> str:
        """
        Construct the DynamoDB primary key for the current database's lock.

        Returns:
            str: The primary key for the lock record in DynamoDB.
        """
        return f"database#{self.database_file_path}"

    def acquire_lock(self) -> None:
        """
        Attempt to acquire a lock for the database in DynamoDB.

        This method tries to acquire a lock by inserting a record into the DynamoDB table.
        It retries multiple times if it fails due to existing locks, using exponential backoff.

        Raises:
            DatabaseBusy: If the lock cannot be acquired after several attempts.
        """
        if self.is_lock_active:
            return
        lock_attempt_count = 0
        delay = 50  # Initial delay in milliseconds
        lock_timeout_deadline = time.time() + self.lock_wait_timeout
        while time.time() < lock_timeout_deadline and lock_attempt_count < self.max_lock_attempts:
            self.current_lock_id = str(uuid.uuid4())
            self.lock_acquired_timestamp = self.current_unix_timestamp
            self.lock_expiry_timestamp = self.lock_acquired_timestamp + self.lock_expiration
            try:
                # Attempt to add a lock record into the DynamoDB table.
                self.dynamodb_lock_table.put_item(
                    Item={
                        'pk': self.dynamodb_primary_key,
                        'lock_id': self.current_lock_id,
                        'expires_at': self.lock_expiry_timestamp
                    },
                    ConditionExpression=(
                        Attr('pk').not_exists() | (
                            Attr('pk').exists() &
                                Attr('expires_at').lt(self.lock_acquired_timestamp)
                        )
                    )
                )
            except ClientError as e:
                logger.warning(
                    "Failed to add lock record to DynamoDB. Key: '%s', Attempt: %d, Error: '%s'.",
                    self.dynamodb_primary_key,
                    lock_attempt_count,
                    e.response['Error']['Code']
                )
            except BotoCoreError as e:
                logger.error(
                    "Failed to add lock record to DynamoDB. Key: '%s', Attempt: %d, Error: '%s'.",
                    self.dynamodb_primary_key,
                    lock_attempt_count,
                    str(e)
                )
            except Exception as e:
                logger.critical(
                    "Failed to add lock record to DynamoDB. Key: '%s', Attempt: %d, Error: '%s'.",
                    self.dynamodb_primary_key,
                    lock_attempt_count,
                    str(e)
                )
            else:
                # Lock successfully acquired.
                logger.info(
                    "Lock acquired: ID '%s', Database '%s', Timestamp %s.",
                    self.current_lock_id,
                    self.database_file_path,
                    self.lock_acquired_timestamp
                )
                return
            # Reset lock variables for next attempt.
            self.current_lock_id = None
            self.lock_acquired_timestamp = None
            self.lock_expiry_timestamp = None
            lock_attempt_count += 1
            # Exponential backoff in seconds.
            time.sleep((delay * lock_attempt_count) / 1000)
        # Lock acquisition failed after all attempts.
        logger.error(
            "Lock acquisition failed: Database '%s', Duration %s seconds, Attempts %d.",
            self.database_file_path,
            self.lock_wait_timeout,
            lock_attempt_count
        )
        raise DatabaseBusy("Failed to acquire database lock.")

    def release_lock(self) -> None:
        """
        Release the database lock in DynamoDB.

        This method attempts to remove the lock record from the DynamoDB table.
        If it fails, the lock will eventually expire on its own, so no exception is raised.

        Logs an error if the release fails, but the lock expiration mechanism ensures eventual
        safety.
        """
        if not self.is_lock_active:
            logger.debug("No active lock to release.")
            return
        try:
            # Attempt to remove the lock record from the DynamoDB table.
            self.dynamodb_lock_table.delete_item(
                Key={
                    'pk': self.dynamodb_primary_key
                },
                ConditionExpression=Attr('lock_id').eq(self.current_lock_id)
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(
                "Lock release failed: ID '%s', Database '%s', Error '%s'.",
                self.current_lock_id,
                self.database_file_path,
                str(e)
            )
        released_at = self.current_unix_timestamp
        logger.info(
            "Lock released: ID '%s', Database '%s', Timestamp %s, Duration %s seconds.",
            self.current_lock_id,
            self.database_file_path,
            released_at,
            released_at - self.lock_acquired_timestamp
        )
        # Reset lock-related attributes.
        self.lock_acquired_timestamp = None
        self.lock_expiry_timestamp = None
        self.is_transaction = False
        self.current_lock_id = None
