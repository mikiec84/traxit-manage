import logging
import os

import click
import pandas as pd

from traxit_manage import config
from traxit_manage.decode import Decode
from traxit_manage.track import Track
from traxit_manage.utility import clean_list_of_files
from traxit_manage.utility import make_db_name
from traxit_manage.utility import path_corpus
from traxit_manage.utility import query_yes_no
from traxit_manage.utility import read_references
from traxit_manage.utility import split_dir_file_ext


logger = logging.getLogger(__name__)
time_format = '%Y-%m-%d-%H-%M-%S'


def ingest_references(corpus,
                      broadcast=None,
                      erase=False,
                      db_class=None,
                      db_name=None,
                      cli=False):
    """Ingest the references defined in the broadcast or a whole corpus*

    To ingest references defined in a broadcast, run ``broadcast_references``.

    Args:
        corpus: the corpus name
        broadcast: the broadcast name. If None, ingest the whole corpus. Defaults to None
        erase (bool): If False, do not erase the database. If True, erase it, but ask for a manual confirmation
            if ``cli`` is True.
        db_class: a db class inheriting from. Can be None
        db_name: the name to instanciate the db with. If None, the name is set to
            ``db_name = make_db_name(corpus, broadcast)``
        cli (bool): show CLI output (loading bar, etc.). Defaults to False.
    """
    if db_name is None:
        db_name = make_db_name(corpus, broadcast)

    corpus_path = path_corpus(corpus)

    references = read_references(corpus_path, broadcast)

    list_of_files = [os.path.join(corpus_path, 'references', filename)
                     for filename in references]

    db_instance = config.configure_database(db_class=db_class,
                                     db_name=db_name)
    if erase:
        if not cli or query_yes_no(u'Are you sure you want to erase the database {db}?'
                                   .format(db=db_instance)):
            db_instance.delete_all()
    else:
        print(u'Using database {db}'.format(db=db_instance))
    list_of_valid = ingest_files(list_of_files,
                                 db_instance,
                                 cli=cli)

    return list_of_files, list_of_valid


def fingerprint_files(filepaths, fingerprinting_instance):
    """Returns the list of valid files to process, and the list of files whose ingestion went wrong.

    Args:
        filepaths (iterator): path to the files to ingest
        fingerprinting_instance: instance of traxit_algorithm.fingerprinting.Fingerprinting
    """
    track_ids_fp_paths = []
    for filepath in filepaths:
        filedir, filename, _ = split_dir_file_ext(filepath)
        if not os.path.exists(os.path.join(filedir, '.fingerprints')):
            os.mkdir(os.path.join(filedir, '.fingerprints'))
        fingerprint_path = os.path.join(filedir, '.fingerprints', filename + '.json')

        track = Track(filepath=filepath)
        track.generate_id()

        track_ids_fp_paths.append((track.id, fingerprint_path))

        if os.path.exists(fingerprint_path):
            logger.info(u'Fingerprint exists for {f}. Skipping.'.format(f=filepath))
            continue
        else:
            logger.info(u'Fingerprinting {f}'.format(f=filepath))
            d = Decode(filepath, keep_buffer=True)
            d.start()
            audio = d.get_data()
            fp = fingerprinting_instance.get_fingerprint(audio, post_process=True)
            fp.to_json(fingerprint_path, 'records')
    return track_ids_fp_paths


def ingest_fingerprints(track_ids_fp_paths, db_instance):
    """Returns the list of valid files to process, and the list of files whose ingestion went wrong.

    Args:
        track_ids_fp_paths (iterator): iterator of tuples (track_id, paths to the fingerprints to ingest)
        db_instance: the instance of the db in which to ingest
    """
    for track_id, fp_path in track_ids_fp_paths:
        logger.info(u'Ingesting fingerprint {f} for track_id {t}'.format(f=fp_path, t=track_id))
        if db_instance.is_ingested_fingerprint(track_id):
            logger.info(u'Already ingested. Skipping.')
            continue
        fp = pd.read_json(fp_path, 'records')
        db_instance.insert_fingerprint(fp, track_id)


def ingest_files(list_of_files,
                 db_instance,
                 cli=False):
    """Returns the list of valid files to process, and the list of files whose ingestion went wrong.

    Args:
        list_of_files: one file path or a list of file paths to ingest
        db_instance: the instance of the db in which to ingest
        cli (bool): show CLI output (loading bar, etc.). Defaults to False.

    Returns:
        a  list of the files after calling ``clean_list_of_files``
    """
    list_of_files = clean_list_of_files(list_of_files)
    fingerprinting_instance = config.configure_fingerprinting()
    if cli:
        with click.progressbar(list_of_files, label='Fingerprinting {0} files'.format(len(list_of_files))) as bar:
            fingerprint_paths = fingerprint_files(bar, fingerprinting_instance)
        with click.progressbar(fingerprint_paths,
                               label='Ingesting {0} fingerprints'.format(len(fingerprint_paths))) as bar:
            ingest_fingerprints(bar, db_instance)
    else:
        track_ids_fp_paths = fingerprint_files(list_of_files, fingerprinting_instance)
        ingest_fingerprints(track_ids_fp_paths, db_instance)
    return list_of_files
