"""Configuration module."""

import logging

from traxit_manage.in_memory_db import DbInMemory
from traxit_manage.utility import _import

logger = logging.getLogger(__name__)


def configure_database(db_name, db_class=None, **kwargs):
    """Configure a database instance.

    If traxit_databases is installed it will defer the configuration to its
    method or otherwise send a warning and use an in-memory database.

    Args:
        db_name: the name of the database to use
        db_class: can be a class, or a fully-qualified class name (as a
          string). Defaults to traxit_databases.config.indexing_db.
    """

    try:
        from traxit_databases.config import configure_database
        return configure_database(db_name, db_class, **kwargs)
    except ImportError:
        logger.warning('traxit_databases is not installed. ')
        if db_class is None:
            logger.warning('Instanciating an in-memory database.')
            return DbInMemory(db_name)
        else:
            return _import(db_class)(db_name)


def configure_fingerprinting(pipeline=None, fingerprinting_class_path=None):
    """Build an instance of a fingerprinting class for a given pipeline.

    Args:
        pipeline (string or dict): If string, then we use the name of the pipeline, which is a key of the
            traxit_algorithm.pipelines.pipelines dictionary. If the pipeline is a dictionary, then we expect it to
            have the same format as the values of the traxit_algorithm.pipelines.pipelines dictionary.
            If pipeline is None (default) then we pick the ``default`` pipeline from traxit_algorithm.

    Returns:
        An instance of a fingerprinting class.

    Raises:
        ValueError: No pipeline was supplied and traxit_algorithm is not installed
        ValueError: The pipeline is not a dict and traxit_algorithm is not installed
    """
    try:
        from traxit_algorithm.config import configure_fingerprinting
        return configure_fingerprinting(pipeline)
    except ImportError:
        logger.warning('traxit_algorithm is not installed. '
                       'You will be missing some cool features.')
        if pipeline is None:
            logger.warning('You should supply your fingerprinting '
                           'algorithm when traxit_algorithm is not '
                           'installed. Choosing the default algorithm.')
            from sample_algorithm import SampleFingerprinting
            pipeline = {
                'fingerprinting': {
                    'class': SampleFingerprinting,
                    'params': None
                }
            }
        if not isinstance(pipeline, dict):
            raise ValueError('The pipeline is not a dict and traxit_algorithm '
                             'is not installed')
        return pipeline['fingerprinting']['class'](
            params=pipeline['fingerprinting']['params']
            )


def configure_matching(pipeline=None,
                       db_instance=None,
                       fingerprinting_instance=None):
    """Build an instance of a matching class for a given pipeline.

    Args:
        pipeline (string or dict): If string, then we use the name of the pipeline, which is a key of the
            traxit_algorithm.pipelines.pipelines dictionary. If the pipeline is a dictionary, then we expect it to
            have the same format as the values of the traxit_algorithm.pipelines.pipelines dictionary.
            If pipeline is None (default) then we pick the ``default`` pipeline.
        db_instance: Instance of a database class which implements query_keys.
        fingerprinting_instance: Instance of a fingerprinting class. If None (default), configure_fingerprinting is
                                 called with the same pipeline argument.

    Returns:
        An instance of a matching class.

    Raises:
        ValueError: No pipeline was supplied and traxit_algorithm is not installed
        ValueError: The pipeline is not a dict and traxit_algorithm is not installed
    """
    try:
        from traxit_algorithm.config import configure_matching
        return configure_matching(pipeline)
    except ImportError:
        logger.warning('traxit_algorithm is not installed. '
                       'You will be missing some cool features.')
        if pipeline is None:
            logger.warning('You should supply your matching '
                           'algorithm when traxit_algorithm is not '
                           'installed. Choosing the default algorithm.')
            from sample_algorithm import SampleMatching
            pipeline = {
                'matching': {
                    'class': SampleMatching,
                    'params': None
                }
            }
        if not isinstance(pipeline, dict):
            raise ValueError('The pipeline is not a dict and traxit_algorithm '
                             'is not installed')
        if fingerprinting_instance is None:
            fingerprinting_instance = None or configure_fingerprinting(pipeline)
        return pipeline['matching']['class'](db_instance,
                                             pipeline['matching']['params'],
                                             fingerprinting=fingerprinting_instance)


def configure_tracklisting(pipeline=None,
                           db_instance=None):
    """Build an instance of a tracklisting class for a given pipeline.

    Args:
        pipeline (string or dict): If string, then we use the name of the pipeline, which is a key of the
            traxit_algorithm.pipelines.pipelines dictionary. If the pipeline is a dictionary, then we expect it to
            have the same format as the values of the traxit_algorithm.pipelines.pipelines dictionary.
            If pipeline is None (default) then we pick the ``default`` pipeline.

    Returns:
        An instance of a tracklisting class.

    Raises:
        ValueError: No pipeline was supplied and traxit_algorithm is not installed
        ValueError: The pipeline is not a dict and traxit_algorithm is not installed
    """
    try:
        from traxit_algorithm.config import configure_tracklisting
        return configure_tracklisting(pipeline)
    except ImportError:
        logger.warning('traxit_algorithm is not installed. '
                       'You will be missing some cool features.')
        if pipeline is None:
            logger.warning('You should supply your tracklisting '
                           'algorithm when traxit_algorithm is not '
                           'installed.')
            from sample_algorithm import SampleTracklisting
            pipeline = {
                'tracklisting': {
                    'class': SampleTracklisting,
                    'params': {
                        'processing_size': 10,
                        'processing_hop': 7,
                        'vote_horizon': 6,
                    }
                }
            }
        if not isinstance(pipeline, dict):
            raise ValueError('The pipeline is not a dict and traxit_algorithm '
                             'is not installed')
        return pipeline['tracklisting']['class'](db_instance, pipeline['tracklisting']['params'])
