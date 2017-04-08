# -*- coding: utf-8 -*-

import glob
import os
import time
import traceback

from click.testing import CliRunner
import pytest


def test_corpus_valid(create_corpus_broadcast):
    cwd, corpus_name, _ = create_corpus_broadcast
    assert os.path.exists(os.path.join(cwd, corpus_name))
    assert os.path.exists(os.path.join(cwd, corpus_name, 'references'))


def test_broadcast_valid(create_corpus_broadcast):
    cwd, corpus_name, broadcast_name = create_corpus_broadcast
    assert os.path.exists(os.path.join(cwd, corpus_name, broadcast_name))
    assert os.path.exists(os.path.join(cwd,
                                       corpus_name,
                                       broadcast_name,
                                       'references.json'))


def test_list_corpus(create_corpus_broadcast):
    from traxit_manage.bin import list_corpus
    runner = CliRunner()
    cwd, corpus_name, _ = create_corpus_broadcast
    corpus_list = ('{corpus_name}: {corpus_path}\n'
                   .format(corpus_name=corpus_name,
                           corpus_path=os.path.join(cwd, corpus_name)))
    assert runner.invoke(list_corpus, []).output == corpus_list


def test_list_broadcast(create_corpus_broadcast):
    from traxit_manage.bin import list_broadcast
    runner = CliRunner()
    cwd, corpus_name, broadcast_name = create_corpus_broadcast
    broadcast_list = ('{broadcast_name}: {broadcast_path}\n'
                      .format(broadcast_name=broadcast_name,
                              broadcast_path=os.path.join(cwd,
                                                          corpus_name,
                                                          broadcast_name)))
    assert runner.invoke(list_broadcast,
                         [corpus_name]).output == broadcast_list


@pytest.mark.xfail
def test_list_broadcast_wrong_corpus(create_corpus_broadcast):
    from traxit_manage.bin import list_broadcast
    runner = CliRunner()
    with pytest.raises(ValueError):
        runner.invoke(list_broadcast, ['foo']).output


def test_clean_references(create_corpus_broadcast):

    from traxit_manage.bin import clean_references
    from traxit_manage.utility import read_references

    runner = CliRunner()
    cwd, corpus_name, _ = create_corpus_broadcast
    references = {}
    output = 'No file set to be deleted.'
    assert output in runner.invoke(clean_references,
                                   [corpus_name, '--dry-run']).output
    # Check that references.json does not exist
    with pytest.raises(IOError):
        read_references(os.path.join(cwd, corpus_name))
    assert output in runner.invoke(clean_references,
                                   [corpus_name]).output
    # Check that references.json was changed
    assert read_references(os.path.join(cwd, corpus_name)) != references


def test_generate_from_broadcast(create_corpus_broadcast_from_tracklist,
                                 ):
    cwd, corpus_name, broadcast_name = create_corpus_broadcast_from_tracklist
    with open(os.path.join(cwd,
                           corpus_name,
                           broadcast_name, 'groundtruth.xml'), 'rb') as f:
        content = f.read()
        assert 'sample2' in content
    with open(os.path.join(cwd,
                           corpus_name,
                           broadcast_name, 'references.json'), 'rb') as f:
        content = f.read()
        assert 'sample2' in content


@pytest.mark.slow
def test_ingest_tracklist_functional(mocker, create_corpus_broadcast_from_tracklist, es_stub):
    es_stub.data['fingerprint'] = []
    from traxit_manage.bin import ingest
    from traxit_manage.bin import tracklist
    from traxit_manage.utility import read_references

    cwd, corpus_name, broadcast_name = create_corpus_broadcast_from_tracklist
    runner = CliRunner()
    result = runner.invoke(ingest,
                           [corpus_name],
                           'y\n')
    if result.exception is not None:
        traceback.print_exception(*result.exc_info)
        assert result.exception is None
    ingestion_output = result.output
    assert 'Fingerprinting 2 files' in ingestion_output
    assert 'Ingesting 2 fingerprints' in ingestion_output
    assert 'Number of input files: 2' in ingestion_output
    assert 'Number of input files that were valid: 2' in ingestion_output
    time.sleep(2)
    result = runner.invoke(tracklist, [corpus_name,
                                       broadcast_name,
                                       '--globaldb'])
    tracklist_output = result.output
    if result.exception is not None:
        traceback.print_exception(*result.exc_info)
        assert result.exception is None
    assert 'Tracklisting in progress' in tracklist_output
    detection_re = os.path.join(cwd, corpus_name, broadcast_name,
                                'detection-*')
    assert len(glob.glob(detection_re)) > 0
    detection_file = glob.glob(detection_re)[0]

    references = read_references(os.path.join(cwd, corpus_name))

    with open(detection_file) as f:
        content = f.read()
        assert references['sample2.mp3'] in content


def test_delete_corpus(create_corpus_broadcast):
    from traxit_manage.bin import delete_corpus
    cwd, corpus_name, _ = create_corpus_broadcast
    runner = CliRunner()
    runner.invoke(delete_corpus, [corpus_name])
    assert not os.path.exists(os.path.join(cwd, corpus_name))


def test_delete_all(create_corpus_broadcast):
    from traxit_manage.bin import delete_corpus
    cwd, corpus_name, _ = create_corpus_broadcast
    runner = CliRunner()
    runner.invoke(delete_corpus, ['--force'])
    assert not os.path.exists(os.path.join(cwd, corpus_name))


def test_delete_broadcast(create_corpus_broadcast):
    from traxit_manage.bin import delete_broadcast
    cwd, corpus_name, broadcast_name = create_corpus_broadcast
    runner = CliRunner()
    runner.invoke(delete_broadcast, [corpus_name, broadcast_name])
    assert not os.path.exists(os.path.join(cwd, corpus_name, broadcast_name))
