import glob
import logging
import os
from shutil import rmtree

import click
from traxit_algorithm.track import Track

from traxit_manage.utility import path_corpus
from traxit_manage.utility import query_yes_no
from traxit_manage.utility import read_corpus
from traxit_manage.utility import read_references
from traxit_manage.utility import split_dir_file_ext
from traxit_manage.utility import write_corpus
from traxit_manage.utility import write_references

logger = logging.getLogger(__name__)


def init_corpus_helper(path, corpus):
    """Creates a new corpus.

    Args:
        path: the path where the corpus should be created.
        corpus: name of the corpus
    """
    corpus_path = os.path.join(path, corpus)
    corpora = read_corpus()
    if corpus in corpora:
        raise ValueError('Corpus already exists')
    if not os.path.exists(corpus_path):
        os.mkdir(corpus_path)
    if not os.path.exists(os.path.join(corpus_path, 'references')):
        os.mkdir(os.path.join(corpus_path, 'references'))
    corpora[corpus] = corpus_path
    write_corpus(corpora)


def list_corpus_helper():
    """List all the corpora on a computer.

    """
    corpora = read_corpus()
    return corpora


def clean_references_helper(corpus, dry_run):
    """Clean the references directory and create a reference.json file.

    It cleans the directory corpus/references and creates a corpus/references.json file consisting
    in {filepath: track_id}. Has to be called after init_corpus.

    Args:
        corpus: name of the corpus
        dry_run (boolean): if True, do not remove the file

    Returns:
        a dictionary of {filepath: reason_for_deletion}
    """
    corpus_path = path_corpus(corpus)
    try:
        references = read_references(corpus_path)
    except IOError:
        references = {}
    references_paths = [os.path.join(corpus_path, 'references', reference)
                        for reference in references]
    all_files = glob.glob(os.path.join(corpus_path, 'references', '*'))
    to_check = set(all_files) - set(references_paths)
    to_delete_paths = set(references_paths) - set(all_files)
    to_delete = [unicode(os.path.basename(to_delete_path))
                 for to_delete_path in to_delete_paths]
    checked_deleted = {}
    with click.progressbar(to_check,
                           label='Checking references') as bar:
        for filepath in bar:
            check_filepath(checked_deleted, dry_run, filepath, references)
    if not dry_run:
        for filename in to_delete:
            if filename in references:
                del references[filename]
        write_references(references, corpus_path)
    return checked_deleted


def check_filepath(checked_deleted, dry_run, filepath, references):
    """Check if a file should be deleted.

    Args:
        checked_deleted (dict): dictionary of {filepath: reason_for_deletion}
        dry_run (boolean): if True, do not remove the file
        filepath (string): path of the file to check
        references (dict): dictionary of references {filepath: track_id}
    """
    try:
        from traxit_manage.settings import audio_filetypes
        _, _, fileextension = split_dir_file_ext(filepath)
        assert (fileextension.lower() in audio_filetypes)
        track = Track(filepath=filepath, generate_id=True)
        references[unicode(os.path.basename(filepath))] = track.id
    except Exception as e:
        try:
            if not dry_run:
                os.remove(filepath)
            checked_deleted[filepath] = str(e)
        except Exception as e2:
            logger.error('Could not remove ({filepath}-{e}): {e2}'
                         .format(filepath=filepath, e=e, e2=e2))


def delete_corpus_helper(corpus=None, force=False):
    """Deletes one or several corpora.

    Args:
        corpus: string if you want to delete a specific corpus, None for
        every corpus.
    """
    if corpus is None:
        if force or query_yes_no('Do you want to delete every working directory?'):
            corpora = read_corpus()
            write_corpus({})
            for _, value in corpora.iteritems():
                rmtree(value)
    else:
        corpora = read_corpus()
        if corpus in corpora:
            try:
                rmtree(corpora[corpus],
                       ignore_errors=False)
            except Exception as exc:
                logger.warning("Could not delete {0}: {1}".format(corpora[corpus], exc))
            del corpora[corpus]
        else:
            raise ValueError('This working directory does not exist.')
        write_corpus(corpora)
