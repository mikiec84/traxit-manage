# -*- coding: utf-8 -*-

'''
@author: Flavian
'''

import logging
import os

import pytest


logger = logging.getLogger(__name__)


@pytest.mark.parametrize('broadcast, nb, expected_filename',
                         [(None, None, 'references.json'),
                          ('broadcast', None, 'references.json'),
                          ('broadcast', 2, 'references_2.json'),
                          ('broadcast', u'héhé', u'references_héhé.json')
                          ])
def test_file_path(broadcast,
                   nb,
                   expected_filename,
                   mock_open_advanced):

    from traxit_manage.utility import file_path

    corpus_path = '/'
    path = file_path(corpus_path, broadcast, nb, 'references')
    if broadcast is None:
        assert path == os.path.join(corpus_path, expected_filename)
    else:
        assert path == os.path.join(corpus_path, broadcast, expected_filename)


def test_get_audio_files_not_cached(mocker):

    from traxit_manage.utility import get_audio_files_not_cached

    mock_glob = mocker.patch('traxit_manage.utility.insensitive_glob')

    def ins_glob(regex):
        if '.mp3' in regex:
            return ['/file.mp3']
        else:
            return ['/file.wav', '/file.mp3.cache']
    mock_glob.side_effect = ins_glob
    audio_files = get_audio_files_not_cached('/', ['mp3', 'wav'], 'cache')
    assert audio_files == ['/file.mp3', '/file.wav']
