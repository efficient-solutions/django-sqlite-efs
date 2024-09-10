"""
settings.py: Utility for retrieving settings from Django's configuration or environment variables.
"""

import os
from typing import Any
from django.conf import settings
from .exceptions import ImproperlyConfigured


def get_setting(key: str, default: Any = None, required: bool = True) -> Any:
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
