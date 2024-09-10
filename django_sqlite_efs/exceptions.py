"""
exceptions.py: Custom exception classes for database and lock management.
"""


class ImproperlyConfigured(Exception):
    """
    Exception raised for errors in the database configuration.

    This exception is used when the database is improperly configured,
    typically indicating a missing or incorrect setting in the configuration.
    """


class LockRequired(Exception):
    """
    Exception raised when a database lock is required but not acquired.

    This exception is raised to signal that a locking operation is required
    for certain database transactions or queries but was not properly obtained.
    """


class DatabaseBusy(Exception):
    """
    Exception raised when the database is currently busy.

    This exception is used to indicate that the database is locked or 
    otherwise busy, typically when another operation is holding a lock 
    and preventing access to the database.
    """
