# -*- coding: utf-8 -*-

from collections import OrderedDict
import StringIO

import pytest

from traxit_manage.utility import csv2listdict
from traxit_manage.utility import dict_to_xml
from traxit_manage.utility import listdict2csv
from traxit_manage.utility import xml_to_dict


@pytest.mark.parametrize("input,expected", [
    ('a;b;c\r\n0.1;"a";{"test1": "test1"}\r\n1;"b";{"test2": "test2"}\r\n',
     [{"a": 0.1, "b": "a", "c": {"test1": "test1"}},
      {"a": 1, "b": "b", "c": {"test2": "test2"}}]),
    ('a;b;c\r\n0.1;a;1\r\n1;b;0.3\r\n', [{"a": 0.1, "b": "a", "c": 1},
                                         {"a": 1, "b": "b", "c": 0.3}])
])
def test_csv2listdict(input, expected):
    csv_file = StringIO.StringIO(input)
    assert csv2listdict(csv_file) == expected


def test_csv2listdict_reciprocity():
    csv_file = StringIO.StringIO()
    original_listdict = [{"a": 0.1, "b": "a", "c": {"test1": "test1"}},
                         {"a": 1, "b": "b", "c": {"test2": "test2"}}]
    listdict2csv(csv_file, original_listdict)
    csv_file_decode = StringIO.StringIO(csv_file.getvalue())
    new_listdict = csv2listdict(csv_file_decode)
    assert original_listdict == new_listdict


@pytest.mark.parametrize("input", [
    {u"root": {u"Hello": u"ça va ?", u"trèsbien": u"à"}},
    {'TrackList': {
        'MusicTrack': [{
            'start': 0,
            'ddex_sources': [],
            'dirpath': u'/tmp/V8Y7wQ7w6STvucAyrCyQpn/test_corpus/references',
            'end': 200,
            'startDate': '2014-01-0100: 00: 00',
            'data': {
                'subtitle': '',
                'contributors': [{
                    'role': 'MainArtist',
                    'name': u'Chromatics'
                }],
                'title': u'KillforLove',
                'duration': 238.0,
                'otherIds': [],
                'details': [{
                    'year': 2012
                }],
                'trackNumber': 2,
                'genre': u'IndieRock/Electronic/Synthpop',
                'ISRC': '',
                'featuring': '',
                'release': {
                    'contributors': [{
                        'role': 'MainArtist',
                        'name': u'Chromatics'
                    }],
                    'title': u'KillForLove'
                }
            },
            'id': '66a0e38a8bef8caceb66fac2ee1025eb',
            'endDate': '2014-01-0100: 03: 20',
            'filename': u'samplé1.mp3'
        }]
    }
    }
])
def test_dict_to_xml(input):
    dict_to_xml(input)


@pytest.mark.parametrize("input,expected", [
    ('''<TrackList>
          <MusicTrack>
              <title>Slo-Mo Girl (Fur Coat Dark After Hour Mix)</title>
          </MusicTrack>
          <MusicTrack>
              <title>Inner Place</title>
          </MusicTrack>
        </TrackList>''',
        OrderedDict([(u'TrackList',
                      OrderedDict([(u'MusicTrack',
                                    [OrderedDict([(u'title', u'Slo-Mo Girl (Fur Coat Dark After Hour Mix)')]),
                                     OrderedDict([(u'title', u'Inner Place')]
                                                 )]
                                    )]
                                  ))
                     ])
     ),
    ('''<Titles>
            <Title TitleType="FormalTitle" TitleFormat="Text">
                <TitleText>Maniac B</TitleText>
                <SubTitle/>
            </Title>
            <Title TitleType="DisplayTitle">
                <TitleText>Maniac B</TitleText>
            </Title>
        </Titles>''',
        OrderedDict([("Titles",
                    OrderedDict([
                                ("Title", [OrderedDict([("@TitleType", "FormalTitle"),
                                                        ("@TitleFormat", "Text"),
                                                        ("TitleText", "Maniac B"),
                                                        ("SubTitle", None)]),
                                           OrderedDict([("@TitleType", "DisplayTitle"),
                                                       ("TitleText", "Maniac B")])])
                                ])
                      )]
                    )),
    ('''<Title TitleType="DisplayTitle">
            the title
        </Title>''',
        OrderedDict([(u'Title', OrderedDict([(u'@TitleType', u'DisplayTitle'), ('#text', u'the title')]))]))
])
def test_xml_to_dict(input, expected):
    assert xml_to_dict(string=input) == expected
