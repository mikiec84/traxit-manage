"""Configuration module."""

import logging

from traxit_manage.in_memory_db import DbInMemory

logger = logging.getLogger(__name__)


def configure_database(db_name, **kwargs):
    """Configure a database instance.

    If traxit_databases is installed it will defer the configuration to its
    method or otherwise send a warning and use an in-memory database.

    Args:
        db_name: the name of the database to use
        db_class: can be a class, or a fully-qualified class name (as a
          string). Defaults to traxit_databases.config.indexing_db.
        store_in: set a path to store the db if it's a local db.
        timeout (float or None): timeout for database requests
    """
    try:
        from traxit_databases.config import configure_database
        return configure_database(args, kwargs)
    except ImportError:
        logger.warning('traxit_databases is not installed. '
                       'Instanciating an in-memory database.')
        return DbInMemory(db_name)
