# -*- coding: utf-8 -*-

'''
@author: Flavian
'''

import logging

import pandas as pd
import pytest


logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def mock_fingerprinting(mocker, fingerprints):
    patcher = mocker.patch('traxit_manage.ingest.configure_fingerprinting')
    patcher.return_value.how_much_audio.return_value = 0, 10
    patcher.return_value.to_index.return_value = 0
    patcher.return_value.get_fingerprint.return_value = fingerprints[0][1]
    patcher.return_value.post_process.return_value = fingerprints[0][1]
    return patcher


@pytest.fixture(scope='function')
def mock_matching(mocker):
    patcher = mocker.patch('traxit_manage.ingest.configure_matching')

    def get_candidates(fp, t1, t2, introspect_trackids=[]):
        return None

    def get_scores(candidates, t1, t2,
                   introspect_trackids=[], fp=[]):
        return None

    patcher.return_value.get_candidates.side_effect = get_candidates
    patcher.return_value.get_scores.side_effect = get_scores
    return patcher


@pytest.fixture(scope='function')
def mock_tracklisting(mocker):
    patcher = mocker.patch('traxit_manage.ingest.configure_tracklisting')
    patcher.return_value.processing_size = 10
    patcher.return_value.processing_hop = 5
    patcher.return_value.history_tracklist = [None, None]
    return patcher


@pytest.mark.parametrize('cli', [True, False])
@pytest.mark.parametrize('db_name', [None, 'test'])
@pytest.mark.parametrize('is_ingested', [True, False])
def test_ingest_references(cli, db_name,
                           is_ingested,
                           create_corpus_broadcast_from_tracklist,
                           mock_db_config,
                           mocker,
                           mock_fingerprinting):
    from traxit_manage.ingest import ingest_references

    from traxit_manage.utility import make_db_name

    mock_db_config.return_value.is_ingested_fingerprint.return_value = is_ingested
    fingerprinting = mock_fingerprinting
    fingerprinting.return_value.get_fingerprint.return_value = pd.DataFrame({'key': [123, 456, 21]})
    mocker.patch('traxit_manage.ingest.Decode')
    cwd, corpus_name, _ = create_corpus_broadcast_from_tracklist
    ingest_references(corpus_name,
                      cli=cli,
                      db_name=db_name,
                      erase=False)
    if is_ingested:
        assert (mock_db_config.return_value.insert_fingerprint.call_args_list == [])
    else:
            assert (len(mock_db_config.return_value.insert_fingerprint.call_args_list) == 2)

    if db_name is None:
        assert (mock_db_config.call_args_list[0][0][0] == make_db_name(corpus_name, None))
    else:
        assert mock_db_config.call_args_list[0][0][0] == 'test'
