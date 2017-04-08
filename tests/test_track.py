# -*- coding: utf-8 -*-

'''
@author: Flavian
'''

import logging

import pytest

from traxit_manage.track import Track


logger = logging.getLogger(__name__)


def test_track(audio_all):
    track = Track(filepath=audio_all)
    track.from_tags()
    track.generate_id()
    assert isinstance(str(track), basestring)
    assert isinstance(repr(track), basestring)


def test_track_as_from(audio_all):
    filepath = audio_all
    if filepath.endswith("wav"):
        return
    track1 = Track(filepath=filepath)
    track1.from_tags()
    track2 = Track(generate_id=False)
    d = track1.as_dict()
    track2.from_dict(d)
    assert track1 == track2


def test_track_as_from_wrong(audio_all):
    filepath = audio_all
    if filepath.endswith("wav"):
        return
    track1 = Track(filepath=filepath)
    track1.from_tags()
    track2 = Track(generate_id=False)
    d = track1.as_dict()
    del d['id']
    with pytest.raises(KeyError):
        track2.from_dict(d)
