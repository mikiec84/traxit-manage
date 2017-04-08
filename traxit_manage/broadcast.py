# -*- coding: utf-8 -*-

import logging
import os
from shutil import rmtree

from traxit_manage.utility import dict_to_xml
from traxit_manage.utility import get_tracklist_from_csv
from traxit_manage.utility import is_broadcast
from traxit_manage.utility import path_corpus
from traxit_manage.utility import query_yes_no
from traxit_manage.utility import read_corpus
from traxit_manage.utility import write_references

logger = logging.getLogger(__name__)
time_format = '%Y-%m-%d-%H-%M-%S'


def init_broadcast_helper(broadcast, corpus):
    """Initialize a broadcast inside a corpus.

    Args:
        broadcast: Name of the broadcast
        corpus: Name of the corpus

    """
    corpora = read_corpus()

    if corpus not in corpora:
        raise ValueError('The corpus you stated does not exist')
    broadcast_path = os.path.join(corpora[corpus], broadcast)
    try:
        os.mkdir(broadcast_path)
    except OSError as e:
        if 'File exists' in e:
            logger.warning('The broadcast dir already exists. Still initializing')
        else:
            raise e
    open(os.path.join(broadcast_path, 'groundtruth.xml'), 'a').close()
    open(os.path.join(broadcast_path, 'references.json'), 'a').close()
    return corpora[corpus], broadcast


def list_broadcast_helper(corpus):
    """List every broadcast in the corpus

    Args:
        corpus: Name of the corpus

    Returns:
        a dict of {broadcast_name: broadcast_path}

    """
    corpora = read_corpus()

    if corpus not in corpora:
        raise ValueError('The working directory you stated does not exist')
    corpus_path = corpora[corpus]
    broadcasts = [os.path.join(corpus_path, item)
                  for item in os.listdir(corpus_path)
                  if item != 'references' and item != 'raw']
    broadcasts = [item for item in broadcasts
                  if os.path.exists(os.path.join(item, 'references.json'))]
    broadcast_dict = {}
    for broadcast in broadcasts:
        # Get only the end of the path
        broadcast_name = os.path.relpath(broadcast, os.path.dirname(broadcast))
        broadcast_dict[broadcast_name] = broadcast
    return broadcast_dict


def generate_from_broadcast_helper(corpus, broadcast, csvFile):
    """Place a csv in your broadcast directory, this will generate grountruth.xml and references.json.

    Args:
        corpus: Name of the corpus
        broadcast: Name of the broadcast
        csvFile: the name of the csv file you put in your broadcast. Example: 'RTL_broadcast.csv'

    """
    corpus_path = path_corpus(corpus)
    if not is_broadcast(corpus_path, broadcast):
        raise ValueError('The broadcast you stated is not valid')
    broadcast_path = os.path.join(corpus_path, broadcast)
    csv_path = os.path.join(corpus_path, broadcast, csvFile)
    tl, references_common = get_tracklist_from_csv(corpus_path, csv_path)
    tl_dict = tl.as_groundtruth()
    xml = dict_to_xml(tl_dict)
    with open(os.path.join(broadcast_path, 'groundtruth.xml'), 'wb') as f:
        f.write(xml)
    write_references(references_common, corpus_path, broadcast)


def delete_broadcast_helper(broadcast=None, corpus=None, force=False):
    """Deletes one or all broadcast(s) from a corpus.

    Args:
        broadcast: Name of the broadcast (default: None). None to delete all broadcast
        corpus: Name of the corpus (default: None)
        force (boolean): do not ask for confirmation on deleting all

    """
    corpora = read_corpus()
    corpus_path = corpora[corpus]
    if broadcast is None:
        if force or query_yes_no('Do you want to delete every '
                                 'broadcast in this directory?'):
            corpora = [os.path.join(corpus_path, item)
                       for item in os.listdir(corpus_path)
                       if item != 'references' and item != 'raw']
            corpora = [item for item in corpora
                       if os.path.exists(os.path.join(item,
                                                      'references.json'))]
            for broadcast in corpora:
                # Get only the end of the path
                rmtree(broadcast,
                       ignore_errors=False)
    else:
        if os.path.exists(os.path.join(corpus_path, broadcast,
                                       'references.json')):
            rmtree(os.path.join(corpus_path, broadcast),
                   ignore_errors=False)
