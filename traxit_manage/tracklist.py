import json
import logging
import os

import click
import pandas as pd

from traxit_manage.config import configure_database
from traxit_manage.config import configure_fingerprinting
from traxit_manage.config import configure_matching
from traxit_manage.config import configure_tracklisting
from traxit_manage.decode import Decode
from traxit_manage.decode import decode_wave
from traxit_manage.decode import length_wave
from traxit_manage.utility import clean_list_of_files
from traxit_manage.utility import dict_to_xml
from traxit_manage.utility import file_path
from traxit_manage.utility import get_audio_files_not_cached
from traxit_manage.utility import json_dump
from traxit_manage.utility import make_db_name
from traxit_manage.utility import path_corpus
from traxit_manage.utility import read_references
from traxit_manage.utility import split_dir_file_ext


logger = logging.getLogger(__name__)

time_format = '%Y-%m-%d-%H-%M-%S'
audio_cache_filetype = 'cache'


def tracklist_helper(corpus,
                     broadcast,
                     reset_cache=False,
                     reset_history_tracklist=False,
                     globaldb=False,
                     db_name=None,
                     cli=False,
                     pipeline=None,
                     detection_file_append='',
                     introspect_trackids=None):
    """Tracklists a file 'audio.*' in the broadcast. Gives an option to export the audio file of the detection.

    Args:
        corpus: name of the corpus
        broadcast: name of the broadcast
        reset_cache: reset everything that is cached. Defaults to False
        reset_history_tracklist: reset only the cached tracklist result. Defaults to False
        globaldb: use a corpus-wide database. Defaults to False
        db_name: the name to instanciate the db with. If None (default), the name is set to
        ``db_name = make_db_name(corpus, broadcast)``
        cli (bool): show CLI output (loading bar, etc.). Defaults to False.
        pipeline: (Optional[string or dict]): If string, then we use the name of the pipeline, which is a key of the
            traxit_algorithm.pipelines.pipelines dictionary. If the pipeline is a dictionary, then we expect it to
            have the same format as the values of the traxit_algorithm.pipelines.pipelines dictionary.
            If pipeline is None (default) then we set the pipeline value to ``default``.
        detection_file_append: an extra string for files produced during the process. Defaults to an empty string
        introspect_trackids: list or None. An introspect.json file will be output in the broadcast folder.

    Returns:
        a dictionary of {audio file name (whithout extension): detection file name}
    """
    if db_name is None:
        if not globaldb:
            db_name = make_db_name(corpus, broadcast)
        else:
            db_name = make_db_name(corpus)
    db_instance = configure_database(db_name=db_name)

    print('Using database {db}'.format(db=db_instance))

    corpus_path = path_corpus(corpus)
    references = read_references(corpus_path, broadcast)

    for filename, track_id in references.iteritems():
        if not db_instance.is_ingested_fingerprint(track_id):
            logger.warning('Fingerprint not ingested for {0}'.format(filename))

    from traxit_manage.settings import audio_filetypes
    audio_files = get_audio_files_not_cached(os.path.join(corpus_path, broadcast),
                                             audio_filetypes, audio_cache_filetype)
    logger.info('Analyzing {0}'.format(audio_files))

    list_of_valid, tls = get_tracklist(audio_files,
                                       db_instance,
                                       corpus_path,
                                       broadcast,
                                       reset_cache=reset_cache,
                                       reset_history_tracklist=reset_history_tracklist,
                                       cli=cli,
                                       pipeline=pipeline,
                                       introspect_trackids=introspect_trackids,
                                       detection_file_append=detection_file_append)
    ok = list(set(list_of_valid))
    detection_dict = {}
    for audio_file_path, tl in zip(ok, tls):
        detection_dict = store_tracklist(broadcast,
                                         corpus_path,
                                         db_name,
                                         detection_dict,
                                         detection_file_append,
                                         audio_file_path,
                                         tl)
    return detection_dict


def store_tracklist(broadcast,
                    corpus_path,
                    db_name,
                    detection_dict,
                    detection_file_append,
                    audio_file_path,
                    tl):
    """Store the tracklist of an audio file from a corpus / broadcast

    Args:
        broadcast: name of the broadcast
        corpus_path: path of the corpus
        db_name: name of the database
        detection_dict: dictionary of {audio file name (whithout extension): detection file name}
        detection_file_append: an extra string for the detection file name
        audio_file_path: path of the audio file
        tl: traxit_algorithm.tracklisting.Tracklist instance

    Returns:
        detection_dict
    """
    _, file_name, _ = split_dir_file_ext(audio_file_path)
    tl_dict = tl.as_detection()
    xml = dict_to_xml(tl_dict)
    detection_filename = (u'{0}.xml'
                          .format('-'.join(filter(None, ['detection',
                                                         db_name,
                                                         file_name,
                                                         detection_file_append]))))
    path_dest = os.path.join(corpus_path, broadcast, detection_filename)
    with open(path_dest, 'wb') as f:
        f.write(xml.encode('utf-8'))
    detection_dict[file_name] = detection_filename
    logger.info(u'Detection file stored at: {0}'.format(f.name))
    return detection_dict


def process_chunk(fingerprinting_instance, matching_instance, tracklisting_instance, filecache, start, end,
                  introspect_trackids):
    """Process an audio segment.

    Args:
        fingerprinting_instance: instance of traxit_algorithm.fingerprinting.Fingerprinting
        matching_instance: instance of traxit_algorithm.matching.Matching
        tracklisting_instance: instance of traxit_algorithm.tracklisting.Tracklisting
        filecache: a file path with extension .cache (which really raw wave)
        start (float): audio segment start time (in seconds)
        end (float): audio segment end time (in seconds)
        introspect_trackids: list or None. An introspect.json file will be output in the broadcast folder.

    Returns:
        pd.DataFrame: Matches for this chunk
    """
    logger.info(u'from {0} to {1}'.format(start, end))
    buf_start, buf_end = fingerprinting_instance.how_much_audio(start, end)
    audio, _ = decode_wave(filecache, buf_start, buf_end, 11025)
    fp = fingerprinting_instance.get_fingerprint(audio, start, end)
    match = matching_instance.get_matches(fp,
                                          start,
                                          end,
                                          introspect_trackids=introspect_trackids,
                                          query_keys_n_jobs=int(os.environ.get('QUERY_N_JOBS', 8)))
    tracklisting_instance.post_processing(match, start, end)
    return match


def get_tracklist(list_of_files,
                  db_instance,
                  corpus_path,
                  broadcast,
                  reset_cache=False,
                  reset_history_tracklist=False,
                  cli=False,
                  pipeline=None,
                  introspect_trackids=None,
                  detection_file_append=''):
    """Compute the tracklists for a list of files.

    Args:
        list_of_files: list of audio file paths
        db_instance: db_instance: the instance of the db with which to tracklist
        corpus_path: path of the corpus
        broadcast: name of the broadcast
        reset_cache: reset everything that is cached. Defaults to False
        reset_history_tracklist: reset only the cached tracklist result. Defaults to False
        cli (bool): show CLI output (loading bar, etc.). Defaults to False.
        pipeline: (Optional[string or dict]): If string, then we use the name of the pipeline, which is a key of the
            traxit_algorithm.pipelines.pipelines dictionary. If the pipeline is a dictionary, then we expect it to
            have the same format as the values of the traxit_algorithm.pipelines.pipelines dictionary.
            If pipeline is None (default) then we set the pipeline value to ``default``.
        introspect_trackids: list or None. An introspect.json file will be output in the broadcast folder.
        detection_file_append: an extra string for files produced during the process. Defaults to an empty string

    Returns:
        a tuple (list_of_files [cleaned], traxit_algorithm.Tracklisting.Tracklist instances)
    """
    # If wave is reset then reset also history
    if reset_cache:
        reset_history_tracklist = True

    list_of_files = clean_list_of_files(list_of_files)
    tls = []
    fingerprinting_instance = configure_fingerprinting(pipeline=pipeline)
    matching_instance = configure_matching(pipeline=pipeline,
                                           fingerprinting_instance=fingerprinting_instance,
                                           db_instance=db_instance)
    tracklisting_instance = configure_tracklisting(pipeline=pipeline,
                                                   db_instance=db_instance)
    for filepath in list_of_files:
        tl = get_tracklist_file(broadcast,
                                cli,
                                corpus_path,
                                filepath,
                                fingerprinting_instance,
                                introspect_trackids,
                                matching_instance,
                                reset_cache,
                                reset_history_tracklist,
                                tracklisting_instance,
                                detection_file_append)
        tls.append(tl)
    return list_of_files, tls


def get_tracklist_file(broadcast, cli, corpus_path, filepath, fingerprinting_instance, introspect_trackids,
                       matching_instance, reset_cache, reset_history_tracklist, tracklisting_instance,
                       detection_file_append):
    """Get the tracklist for one file.

    Args:
        broadcast: name of the broadcast
        cli (bool): show CLI output (loading bar, etc.). Defaults to False.
        corpus_path: path of the corpus
        filepath: path to the file to tracklist
        fingerprinting_instance: instance of traxit_algorithm.fingerprinting.Fingerprinting
        introspect_trackids: list or None. An introspect.json file will be output in the broadcast folder.
        matching_instance: instance of traxit_algorithm.matching.Matching
        reset_cache: reset everything that is cached. Defaults to False
        reset_history_tracklist: reset only the cached tracklist result. Defaults to False
        tracklisting_instance: instance of traxit_algorithm.tracklisting.Tracklisting
        introspect_trackids: list or None. An introspect.json file will be output in the broadcast folder.
        detection_file_append: an extra string for files produced during the process.

    Returns:
        traxit_algorithm.Tracklisting.Tracklist: Tracklist of the file

    """
    matches = []
    filecache = filepath + '.' + audio_cache_filetype
    _, filename_no_ext, _ = split_dir_file_ext(filepath)
    logger.info('Caching decoded {0} into {1}'.format(filepath, filecache))
    if reset_cache and os.path.exists(filecache):
        os.remove(filecache)
    if not os.path.exists(filecache):
        d = Decode(filepath, mode='filewavsink',
                   location_store=filecache)
        d.start()
    matches_saved = file_path(corpus_path, broadcast, '_'.join((filename_no_ext, detection_file_append)), 'matches')
    tracklist_saved = file_path(corpus_path, broadcast, '_'.join((filename_no_ext, detection_file_append)), 'tracklist')
    if reset_history_tracklist and os.path.exists(tracklist_saved):
        os.remove(tracklist_saved)
    end_file = length_wave(filecache)
    if not os.path.exists(tracklist_saved):
        tracklisting_instance.reset()
        logger.info('Starting tracklisting with length {0}s'.format(end_file))
        # In order to mimic a do() while.
        if cli:
            with click.progressbar(tracklisting_instance.times_list(end_file),
                                   label='Tracklisting in progress') as bar:
                for start, end in bar:
                    matches.append(process_chunk(fingerprinting_instance,
                                                 matching_instance,
                                                 tracklisting_instance,
                                                 filecache, start, end,
                                                 introspect_trackids))
        else:
            for start, end in tracklisting_instance.times_list(end_file):
                matches.append(process_chunk(fingerprinting_instance,
                                             matching_instance,
                                             tracklisting_instance,
                                             filecache, start, end,
                                             introspect_trackids))
        with open(matches_saved, 'wb+') as f:
            pd.concat(matches).to_json(f, 'records')
        with open(tracklist_saved, 'wb+') as f:
            json.dump(tracklisting_instance.history_tracklist, f)
    else:
        with open(tracklist_saved, 'rb') as f:
            tracklisting_instance.history_tracklist = json.load(f)
    tl = tracklisting_instance.get_tracklist(end_file)
    if introspect_trackids:
        with open(os.path.join(corpus_path, broadcast,
                               'introspect-{0}.json'.format(filename_no_ext)), 'wb+') as f:
            json_dump(matching_instance.introspection, f)
    return tl
