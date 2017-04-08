from mock import MagicMock
import pandas as pd
import pytest

from traxit_manage.tracklist import get_tracklist
from traxit_manage.tracklist import store_tracklist
from traxit_manage.tracklist import tracklist_helper


@pytest.mark.parametrize('global_db', [True, False])
def test_tracklist_helper(mocker, global_db):
    corpus = 'corpus'
    broadcast = 'broadcast'
    mocker.patch('traxit_manage.tracklist.path_corpus',
                 return_value='/somepath')
    mocker.patch('traxit_manage.tracklist.read_references',
                 return_value={'afile': 'anid'})
    mock_db = mocker.patch('traxit_manage.tracklist.configure_database')
    mock_db.return_value.is_ingested_fingerprint.return_value = False
    mocker.patch('__builtin__.open')
    mocker.patch('traxit_manage.tracklist.get_audio_files_not_cached',
                 return_value=['a_file'])
    mocker.patch('traxit_manage.tracklist.get_tracklist',
                 return_value=(['okfile'], ['oktl']))
    mocker.patch('traxit_manage.tracklist.store_tracklist')
    tracklist_helper(corpus,
                     broadcast,
                     reset_cache=False,
                     reset_history_tracklist=False,
                     globaldb=global_db,
                     db_name=None,
                     cli=False,
                     detection_file_append='')


@pytest.mark.parametrize('detection_file_append', ['', 'append'])
def test_store_tracklist(mocker, detection_file_append):
    broadcast = 'broadcast'
    corpus_path = 'corpus_path'
    db_name = 'db_name'
    detection_dict = {}
    tl = MagicMock()
    audio_file_path = '/somedirectory/file.mp3'

    mocker.patch('traxit_manage.tracklist.dict_to_xml')
    mocker.patch('__builtin__.open')

    store_tracklist(broadcast,
                    corpus_path,
                    db_name,
                    detection_dict,
                    detection_file_append,
                    audio_file_path,
                    tl)

    if detection_file_append:
        assert detection_dict == {'file': 'detection-db_name-file-append.xml'}
    else:
        assert detection_dict == {'file': 'detection-db_name-file.xml'}


@pytest.mark.parametrize('cli', [True, False])
@pytest.mark.parametrize('reset_cache', [True, False])
@pytest.mark.parametrize('reset_history_tracklist', [True, False])
@pytest.mark.parametrize('paths_exist', [True, False])
def test_get_tracklist(mocker,
                       cli,
                       reset_cache,
                       reset_history_tracklist,
                       paths_exist):
    list_of_files = ['/filepath.mp3']
    corpus_path = '/the_corpus_path'
    broadcast = 'broadcast'
    db_instance = MagicMock()

    mocker.patch('json.load')
    mocker.patch('json.dump')
    mocker.patch('__builtin__.open')
    mocker.patch('os.path.exists', return_value=paths_exist)
    mock_os_remove = mocker.patch('os.remove')
    mocker.patch('traxit_manage.tracklist.clean_list_of_files',
                 return_value=list_of_files)
    mocker.patch('traxit_manage.tracklist.decode_wave',
                 return_value=('audio', True))
    mocker.patch('traxit_manage.tracklist.length_wave')
    mocker.patch('traxit_manage.tracklist.Decode')
    mock_fingerprinting = mocker.patch('traxit_manage.tracklist.configure_fingerprinting').return_value
    mock_fingerprinting.how_much_audio.return_value = (0, 10)
    mock_matching = mocker.patch('traxit_manage.tracklist.configure_matching').return_value
    mock_matching.get_matches.return_value = pd.DataFrame({'track_id': ['A', 'B'], 'score': [10, 2]})
    mock_tracklisting = mocker.patch('traxit_manage.tracklist.configure_tracklisting').return_value
    mock_tracklisting.times_list.return_value = [(0, 10)]
    mock_tracklisting.get_tracklist.return_value = 'tracklist'

    get_tracklist(list_of_files,
                  db_instance,
                  corpus_path,
                  broadcast,
                  reset_cache=reset_cache,
                  reset_history_tracklist=reset_history_tracklist,
                  cli=cli)

    if reset_cache and paths_exist:
        assert len(mock_os_remove.call_args_list) == 2

    elif reset_history_tracklist and paths_exist:
        assert len(mock_os_remove.call_args_list) == 1
