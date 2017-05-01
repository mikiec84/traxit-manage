# -*- coding: utf-8 -*-

'''
@author: Flavian
'''

import logging
import os
import pandas as pd
import shutil

from click.testing import CliRunner
from mock import mock_open as _mock_open
import pytest
from shortuuid import uuid

from traxit_manage.utility import listdict2csv


logger = logging.getLogger(__name__)


@pytest.yield_fixture(scope='function', autouse=True)
def settings(mocker):
    cwd = '/tmp/' + uuid()
    os.mkdir(cwd)
    mocker.patch.dict('os.environ', dict(
        QUERY_N_JOBS='1',  # Very important since mock_es is not thread-safe
        DATA_FOLDER=os.environ.get('DATA_FOLDER', cwd),
        LOG_FOLDER=os.environ.get('LOG_FOLDER', os.path.join(cwd, 'log')),
        SHARED_MONGO_URI=os.environ.get('SHARED_MONGO_URI', 'mongodb://localhost:27017/MongoLab-99'),
        ES_HOSTS=os.environ.get('ES_HOSTS', 'localhost:9200'),
        STORAGE_ACCOUNT_NAME=os.environ.get('STORAGE_ACCOUNT_NAME', 'traxitlocal'),
        STORAGE_PRIMARY_KEY=os.environ.get('STORAGE_PRIMARY_KEY', 'voloCdWuwvJ83aw0QQN+22yM9QJMbIIElNr'
                                                                  '9Z5aHONGkWuwtjwjOJ/cydG2IYDSAssqdb9'
                                                                  'T7Z86UvI1am1lL8Q==')
    ))
    import traxit_manage.settings
    reload(traxit_manage.settings)
    yield cwd
    shutil.rmtree(cwd)


def pytest_addoption(parser):
    parser.addoption('--runslow', action='store_true',
                     help='run slow tests')


def pytest_runtest_setup(item):
    if 'slow' in item.keywords and not item.config.getoption('--runslow'):
        pytest.skip('need --runslow option to run')


@pytest.fixture(scope='function')
def mock_db_config(mocker):
    patcher = mocker.patch('traxit_manage.config.configure_database').return_value
    return patcher


@pytest.fixture(scope='function')
def mock_open_advanced(mocker):
    # advanced > regular: http://stackoverflow.com/questions/1289894/how-do-
    # i-mock-an-open-used-in-a-with-statement-using-the-mock-framework-in-pyth
    return mocker.patch('__builtin__.open', _mock_open())


@pytest.yield_fixture(scope='function')
def cwd(request, monkeypatch):
    cwd = '/tmp/' + uuid()
    os.mkdir(cwd)
    monkeypatch.setattr('os.getcwd', lambda: cwd)
    monkeypatch.setattr('os.getcwdu', lambda: unicode(cwd))
    monkeypatch.setattr('traxit_manage.settings.DATA_FOLDER',
                        os.path.join(cwd))
    monkeypatch.setattr('traxit_manage.settings.working_directories_file',
                        os.path.join(cwd, 'corpus.json'))
    yield cwd

    shutil.rmtree(cwd)


@pytest.yield_fixture()
def create_corpus_broadcast(audio_creep_no_unicode,
                            audio_lady_unicode,
                            cwd):
    from traxit_manage.bin import init_broadcast
    from traxit_manage.bin import init_corpus
    runner = CliRunner()
    corpus_name = 'test_corpus' + uuid()
    broadcast_name = 'test_broadcast' + uuid()
    os.mkdir(os.path.join(cwd, corpus_name))
    os.mkdir(os.path.join(cwd, corpus_name, broadcast_name))
    runner.invoke(init_corpus, [corpus_name])
    runner.invoke(init_broadcast, [corpus_name, broadcast_name])
    src = audio_lady_unicode
    dst = os.path.join(cwd, corpus_name, 'references')
    if os.path.exists(dst):
        shutil.copy(src, dst)
    src = audio_creep_no_unicode
    dst = os.path.join(cwd, corpus_name, 'references')
    if os.path.exists(dst):
        shutil.copy(src, dst)
    yield cwd, corpus_name, broadcast_name

    from traxit_manage.config import configure_database
    from traxit_manage.utility import make_db_name

    db_name = make_db_name(corpus_name, broadcast_name)
    db_instance = configure_database(db_class=None,
                                     db_name=db_name)
    db_instance.delete_all()


@pytest.fixture()
def create_corpus_broadcast_from_tracklist(create_corpus_broadcast,
                                           audio_creep_no_unicode):
    from traxit_manage.bin import clean_references
    from traxit_manage.bin import generate_from_broadcast

    cwd, corpus_name, broadcast_name = create_corpus_broadcast
    runner = CliRunner()
    runner.invoke(clean_references, [corpus_name])
    filename = os.path.basename(audio_creep_no_unicode)
    with open(os.path.join(cwd,
                           corpus_name,
                           broadcast_name, 'tracklist.csv'), 'wb+') as f:
        listdict2csv(f, [{'filename': filename,
                          'start': 0, 'end': 200}])
    runner.invoke(generate_from_broadcast, [corpus_name,
                                            broadcast_name,
                                            'tracklist.csv'])
    src = audio_creep_no_unicode
    dst = os.path.join(cwd, corpus_name, broadcast_name)
    if os.path.exists(dst):
        shutil.copy(src, dst)
    return cwd, corpus_name, broadcast_name


@pytest.fixture(scope='session')
def sample_files():
    return os.path.join(os.getcwdu(), 'sample_files')


@pytest.fixture(scope='session', params=[u'samplé1.mp3', u'sample2.mp3',
                                         u'sample.2.mp3', u'wavefile.wav'])
def audio_all(request, sample_files):
    return os.path.join(sample_files, request.param)


@pytest.fixture(scope='session')
def audio_creep_no_unicode(sample_files):
    return os.path.join(sample_files, u'sample2.wav')


@pytest.fixture(scope='session')
def audio_lady_unicode(sample_files):
    return os.path.join(sample_files, u'samplé1.wav')


_fp1 = [{'detuning': 20, 'key': 1}, {'detuning': 30, 'key': 2},
        {'detuning': 40, 'key': 4}, {'detuning': 50, 'key': 1}]
_fp2 = [{'detuning': 20, 'key': 1}, {'detuning': 30, 'key': 2},
        {'detuning': 40, 'key': 3}, {'detuning': 50, 'key': 1}]


@pytest.fixture(scope='session')
def fingerprints(request):
    fingerprints = []
    for fingerprint in [_fp1, _fp2]:
        track_id = uuid()
        fingerprints.append((track_id, fingerprint))
    return fingerprints


fp1 = pd.DataFrame([{'detuning': 20, 'key': 1}, {'detuning': 30, 'key': 2},
                    {'detuning': 40, 'key': 4}, {'detuning': 50, 'key': 1}])
fp2 = pd.DataFrame([{'detuning': 20, 'key': 1}, {'detuning': 30, 'key': 2},
                    {'detuning': 40, 'key': 3}, {'detuning': 50, 'key': 1}])


# Only one key common to both
fp3 = pd.DataFrame([{'detuning': 20, 'key': 1}, {'detuning': 30, 'key': 2},
                    {'detuning': 40, 'key': 4}, {'detuning': 50, 'key': 1}])
fp4 = pd.DataFrame([{'detuning': 20, 'key': 1}, {'detuning': 30, 'key': 5},
                    {'detuning': 40, 'key': 3}, {'detuning': 50, 'key': 1}])


@pytest.fixture(scope='session')
def fingerprint_typeI():
    fingerprints = []
    track_id = uuid()
    fingerprints.append((track_id, pd.DataFrame([{"key": "0" * 116, "detuning": 0},
                        {"key": "1" * 116, "detuning": 0},
                        {"key": "0" * 116, "detuning": 0},
                        {"key": "1" * 116, "detuning": 0}])))
    return fingerprints


@pytest.fixture(scope='session', params=[fp1, fp2])
def fingerprint(request):
    track_id = uuid()
    return track_id, request.param


@pytest.fixture(scope='session')
def fingerprints(request):
    fingerprints = []
    for fingerprint in [fp1, fp2]:
        track_id = uuid()
        fingerprints.append((track_id, fingerprint))
    return fingerprints


@pytest.fixture(scope='session')
def fingerprints2(request):
    fingerprints = []
    for fingerprint in [fp3, fp4]:
        track_id = uuid()
        fingerprints.append((track_id, fingerprint))
    return fingerprints


@pytest.fixture(scope='session')
def match_sample():
    return pd.DataFrame([{'match': 'cool'}])

@pytest.fixture(scope='function')
def db_name():
    return uuid()

@pytest.fixture(scope='function', params=['elasticsearch'])
def traxit_db(request, db_name):
    from traxit_manage.in_memory_db import DbInMemory
    db = DbInMemory(db_name)

    def fin():
        db.delete_all()

    request.addfinalizer(fin)
    return db
