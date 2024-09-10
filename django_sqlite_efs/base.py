"""
base.py: Custom Django database backend for SQLite with Amazon EFS support.

This module provides a custom database wrapper for SQLite, enabling support
for Amazon EFS and distributed locking using DynamoDB.
"""

import logging

from django.db.backends.sqlite3 import base
from django.utils.asyncio import async_unsafe

from .lock_manager import DynamoDBLockManager
from .exceptions import LockRequired

logger = logging.getLogger(__name__)

# INIT_COMMAND: SQLite configuration PRAGMAs to optimize performance for Amazon EFS.
INIT_COMMAND : str = (
    "PRAGMA synchronous = EXTRA;"  # Prioritize data integrity over speed
    "PRAGMA temp_store = MEMORY;"  # Use memory for temporary storage
    "PRAGMA cache_spill = FALSE;"  # Prevent spilling of cache to disk
    "PRAGMA cache_size = -268435456;"  # Use up to 256 MB for cache (negative = bytes)
    "PRAGMA mmap_size = 268435456;"  # Use memory-mapped I/O for up to 256 MB
)


class DatabaseWrapper(base.DatabaseWrapper):
    """
    Custom database wrapper for SQLite with EFS support and distributed locking.

    This class extends Django's SQLite backend to add support for Amazon EFS 
    and implements a distributed locking mechanism using DynamoDB.
    """

    def __init__(self, settings_dict, *args, **kwargs):
        """
        Initialize the DatabaseWrapper with custom settings for SQLite.

        Modifies the connection settings to disable pooling and set specific 
        SQLite PRAGMAs for optimizing the performance on Amazon EFS. Also, 
        initializes the DynamoDBLockManager for distributed locking.

        Args:
            settings_dict (dict): The Django database settings dictionary.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        # Disable connection pooling to ensure each query gets a fresh connection.
        settings_dict['CONN_MAX_AGE'] = 0
        # Ensure 'OPTIONS' is present in settings_dict.
        if 'OPTIONS' not in settings_dict:
            settings_dict['OPTIONS'] = {}
        # Set the SQLite init commands for optimized performance if not already set.
        if 'init_commands' not in settings_dict['OPTIONS']:
            settings_dict['OPTIONS']['init_command'] = INIT_COMMAND
        # Call the parent constructor.
        super().__init__(settings_dict, *args, **kwargs)
        # Initialize the lock manager for distributed locking via DynamoDB.
        self.lock_manager = DynamoDBLockManager(
            database_file_path=settings_dict['NAME'],
            lock_wait_timeout=settings_dict['OPTIONS'].get('timeout')
        )

    def create_cursor(self, name=None):
        """
        Create a database cursor with distributed locking support.

        This method wraps the SQLite cursor with additional functionality 
        to acquire and release locks via DynamoDB.

        Args:
            name (str, optional): An optional cursor name. Defaults to None.

        Returns:
            EFSCursorWrapper: A custom cursor wrapper with EFS locking support.
        """
        return EFSCursorWrapper(
            cursor=self.connection,
            lock_manager=self.lock_manager
        )

    @async_unsafe
    def connect(self):
        """
        Establish a new database connection with locking mechanism.

        Extends the default connect() method to ensure a distributed lock 
        is acquired before establishing the SQLite connection. The lock is 
        released once the connection is successfully opened.

        This prevents race conditions and ensures the database connection 
        remains consistent when accessed across multiple distributed systems.
        """
        if self.lock_manager.rollback_journal_exists():
            # If a rollback journal exists, it indicates an active or failed
            # transaction. A lock is required to ensure safe database access.
            logger.warning(
                "Rollback journal found. Acquiring lock before opening new database connection."
            )
            self.lock_manager.acquire_lock()
        # Establish the database connection.
        super().connect()
        # Release the lock after the connection is successfully established.
        self.lock_manager.release_lock()

    @async_unsafe
    def close(self):
        """
        Close the database connection with proper locking.

        Acquires a distributed lock before closing the connection to ensure 
        that no data corruption occurs due to unfinished transactions. If a 
        rollback journal is detected, the connection will not be closed 
        since it's unsafe to proceed.

        The lock is released after the connection is successfully closed.
        """
        if self.lock_manager.is_transaction is True:
            # Acquire a lock before closing the connection if a transaction
            # is still in progress.
            self.lock_manager.acquire_lock()
        elif self.lock_manager.rollback_journal_exists():
            # If a rollback journal exists, another transaction may be in
            # progress in a different request, making it unsafe to close
            # the connection.
            logger.warning(
                "Rollback journal exists. Skip database connection closure."
            )
            return
        # Close the database connection.
        super().close()
        # Release the lock after closing the connection.
        self.lock_manager.release_lock()

    @async_unsafe
    def commit(self):
        """
        Commit the transaction with distributed locking.

        This method ensures a distributed lock is held during the commit 
        operation, preventing data inconsistency. If no lock is active, a 
        LockRequired exception is raised.

        Raises:
            LockRequired: If no active lock is found during commit.
        """
        if not self.lock_manager.is_lock_active:
            raise LockRequired("Database lock is required for transaction commit.")
        # Commit the transaction.
        super().commit()
        # Log any errors or release the lock after a successful commit.
        if self.errors_occurred:
            logger.error("Transaction commit failed. Database lock retained.")
        else:
            logger.debug("Transaction committed successfully. Releasing database lock.")
            self.lock_manager.release_lock()

    @async_unsafe
    def rollback(self):
        """
        Rollback the transaction with distributed locking.

        Ensures a distributed lock is held during the rollback operation. 
        If no active lock is found, raises a LockRequired exception to 
        prevent potential data corruption.

        Raises:
            LockRequired: If no active lock is found during rollback.
        """
        if not self.lock_manager.is_lock_active:
            raise LockRequired("Database lock is required for transaction rollback.")
         # Rollback the transaction.
        super().rollback()
        # Log any errors or release the lock after a successful rollback.
        if self.errors_occurred:
            logger.error("Transaction rollback failed. Database lock retained.")
        else:
            logger.debug("Transaction rolled back successfully. Releasing database lock.")
            self.lock_manager.release_lock()


class EFSCursorWrapper(base.SQLiteCursorWrapper):
    """
    Custom cursor wrapper with distributed locking support for SQLite on EFS.

    This class extends Django's SQLiteCursorWrapper to add a locking mechanism
    using DynamoDB, specifically designed for databases stored on Amazon EFS.
    It ensures that every query execution is protected by a distributed lock,
    preventing race conditions and ensuring data integrity.
    """

    def __init__(self, cursor, lock_manager, *args, **kwargs):
        """
        Initialize the EFSCursorWrapper with the provided cursor and lock manager.

        This constructor stores the underlying database cursor and the
        DynamoDBLockManager for managing distributed locks.

        Args:
            cursor: The underlying SQLite cursor provided by the database backend.
            lock_manager (DynamoDBLockManager): Instance of the lock manager 
                responsible for handling the distributed locks.
            *args: Additional positional arguments for the parent class.
            **kwargs: Additional keyword arguments for the parent class.
        """
        super().__init__(cursor, *args, **kwargs)
        self.lock_manager = lock_manager

    def execute(self, query, params=None):
        """
        Execute a single SQL query with distributed locking.

        This method ensures that a distributed lock is acquired before
        executing the provided SQL query. It releases the lock once the
        query execution is complete.

        Args:
            query (str): The SQL query to be executed.
            params (tuple, optional): Parameters to be passed with the query. 
                Defaults to None if no parameters are provided.

        Returns:
            Any: The result of the query execution, which may vary depending on
            the SQL query being executed (e.g., rows fetched, row count).
        """
        # Acquire the distributed lock and execute the query within its context.
        with self.lock_manager.set_query_for_context(query) as lock:
            logger.debug("Executing query: '%s'.", lock.current_sql_query)
            return super().execute(query, params)

    def executemany(self, query, param_list):
        """
        Execute a batch of SQL queries with distributed locking.

        This method ensures that a distributed lock is acquired before
        executing multiple SQL queries in batch mode. It releases the lock
        after all queries in the batch have been executed.

        Args:
            query (str): The SQL query template to execute. It will be executed
                multiple times with different parameters.
            param_list (list): A list of tuples, where each tuple contains the
                parameters to be passed with each execution of the query.

        Returns:
            Any: The result of the batch execution, which may vary depending on
            the SQL queries being executed (e.g., affected row counts).
        """
        with self.lock_manager.set_query_for_context(query) as lock:
            logger.debug("Executing multiple queries: '%s'.", lock.current_sql_query)
            return super().executemany(query, param_list)
