"""Module for loading, and accessing the configuration.

"""

import configparser
import typing

_CONFIGURATION_FILE = 'config.ini'
"""Configuration file path."""

_config: typing.Optional[configparser.ConfigParser] = None
"""Configuration singleton object."""


def get_config() -> configparser.ConfigParser:
    """Get the configuration.

    Returns
    -------
    configparser.ConfigParser
        The configuration object.

    """
    global _config
    if not _config:
        _config = configparser.ConfigParser()
        _config.read(_CONFIGURATION_FILE)
    return _config
