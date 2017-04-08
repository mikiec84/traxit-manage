"""
Created on dec. 17 2013

@author: Flavian
"""
import base64
import copy
import hashlib
import logging
import os
import pprint

from hsaudiotag import auto

logger = logging.getLogger(__name__)

data_template = {'title': '',
                 'subtitle': '',
                 'featuring': '',
                 'duration': 0.,
                 'contributors': [{'name': '',
                                   'role': ''}],
                 'release': {'title': '',
                             'UPC': None,
                             'EAN': None,
                             'label': '',
                             'genre': '',
                             'physicalReleaseDate': None,
                             'digitalReleaseDate': None,
                             'subtitle': None,
                             'cover': None,
                             'contributors': [{'name': '',
                                               'role': ''}],
                             'exclude': [],
                             'type': 'Unknown'
                             # See http://ddex.net/dd/DDEX-ERN-341-DD/dd/ddex_ReleaseType.html for a list
                             },
                 'trackNumber': 0,
                 'ISRC': '',
                 'genre': '',
                 'details': None,
                 'otherIds': []}

FX_attributes_template = {'eq': {},
                          'pitch': 0,
                          'stretch': 0,
                          'amp': 0.,
                          'media_start': None,
                          'media_duration': None}


def unique_tracks(seq):
    # order preserving
    checked = []
    for e in seq:
        if e not in checked:
            checked.append(e)
        else:
            f = checked[checked.index(e)]
            f.ddex_sources.extend(e.ddex_sources)
            f.ddex_sources.sort()
    return checked


class Track(object):
    """A Track object

    Attributes:
        id (str): Echo Nest Track ID

        title (str): Track Title

        artist_name (str): Artist Name

        artist_id (str): Artist ID

        score(int): score when matched

    """

    @staticmethod
    def from_dicts(*dicts):
        tracks = [Track()] * len(dicts)
        for i, d in enumerate(dicts):
            tracks[i].from_dict(d)
        return tracks

    @staticmethod
    def eq_core_non_list(track1, track2):
        return all(track1.data[core] == track2.data[core]
                   for core in track1.data
                   if not isinstance(track1.data[core], list))

    def __init__(self, filepath=u'', ddex_source=None, hash_id=True,
                 generate_id=False,
                 **kwargs):
        """Initialize an empty Track instance

        Args:
            filepath (unicode): Path to the audio file.

        Returns:
            A Track object
        """
        if not isinstance(filepath, unicode):
            filepath = filepath.decode('utf-8')
        self.filename = os.path.basename(filepath)
        self.dirpath = os.path.dirname(filepath)
        if ddex_source:
            self.ddex_sources = [ddex_source]
        else:
            self.ddex_sources = []
        if generate_id:
            self.generate_id(hash_id=hash_id)
        else:
            self.id = None
        self.data = copy.deepcopy(data_template)
        self.FX_attributes = copy.deepcopy(FX_attributes_template)
        self.set_fx(**kwargs)

    def __repr__(self):
        return pprint.pformat(self.as_dict())

    def __str__(self):
        if not self.data['title'] and not self.data['contributors']:
            raise ValueError('No title or contributors set, call from_kwargs(filename) or from_tags().')
        if not (hasattr(self, 'id')):
            raise ValueError('No track_id set, call generate_id().')
        return u'<Track \'{0}\' (from {1}) - track_id: {2}>'.format(self.data['title'],
                                                                    ', '.join([contributor['name'] for contributor in
                                                                               self.data['release']['contributors']]),
                                                                    self.id)

    def __eq__(self, other):
        """Compares two track instances. Do not forget to normalize first!

        """
        return self.data == other.data

    def get_filepath(self):
        """Returns the file path

        """
        return os.path.join(self.dirpath, self.filename)

    def set_fx(self, **kwargs):
        """Set fx on a track

        """
        for kwarg in kwargs:
            if kwarg in self.FX_attributes:
                self.FX_attributes[kwarg] = kwargs[kwarg]

    def normalize(self):
        """Changes list into a sorted list. This is useful for track comparison.

        """
        for core in self.data:
            if isinstance(self.data[core], list):
                self.data[core] = sorted(self.data[core])

    def as_dict(self, include_FX=False):
        """Outputs the Track as a dictionary. Set include_FX to True to output sound effects as well.

        If generate_id was not called, it will generate a hash id.
        If from_tags or from_kwargs was not called, then the core attributes
        will have their default empty values.
        """
        if not self.id:
            logger.warning(u'Beware, no track id was set! Setting a hash now')
            self.generate_id()
        if include_FX:
            return {'id': self.id,
                    'ddex_sources': self.ddex_sources,
                    'data': self.data,
                    'filename': self.filename,
                    'dirpath': self.dirpath,
                    'FX': self.FX_attributes}
        else:
            return {'id': self.id,
                    'ddex_sources': self.ddex_sources,
                    'filename': self.filename,
                    'dirpath': self.dirpath,
                    'data': self.data}

    def from_dict(self, as_dict, include_FX=False):
        """Takes as input a dictionary generated by as_dict.

        Raises:
             KeyError: the dictionary has the wrong format
        """
        self.id = as_dict['id']
        self.ddex_sources = as_dict['ddex_sources']
        self.data = as_dict['data']
        self.filename = as_dict.get('filename', self.filename)
        self.dirpath = as_dict.get('dirpath', self.dirpath)
        if include_FX:
            self.FX_attributes = as_dict['FX']

    def from_tags(self):
        # artist - album - title - genre - year - track
        # duration
        tags = auto.File(os.path.join(self.dirpath, self.filename))
        if tags.valid:
            self.data['duration'] = float(tags.duration)
            self.data['contributors'] = [{'name': tags.artist,
                                          'role': 'MainArtist'}]
            self.data['release'] = {'title': tags.album,
                                    'contributors': [{'name': tags.artist,
                                                      'role': 'MainArtist'}]}
            self.data['trackNumber'] = tags.track
            self.data['title'] = tags.title
            self.data['genre'] = tags.genre
            self.data['details'] = []
            try:
                self.data['details'] = [{'year': int(tags.year)}]
            except ValueError as exc:
                # ValueError is raised when tags.year cannot be converted to int
                logger.warning(u'Cannot convert year: {0} to int ({1})'.format(tags.year, str(exc)))
        else:
            logger.warning(u'Could not read the tags for this file: {0}'
                           .format(os.path.join(self.dirpath, self.filename)))

    def _hashfile(self, filename, block_size=65536, hexdigest=True):
        """Hash a file.

        Example use: hashfile(open(fname, 'rb'))
        """
        sha256 = hashlib.sha256()
        with open(filename, 'rb') as f:
            buf = f.read(block_size)
            while buf:
                sha256.update(buf)
                buf = f.read(block_size)
            if hexdigest:
                return sha256.hexdigest()
            else:
                return base64.b64encode(sha256.digest())

    def generate_id(self, hash_id=True):
        if hasattr(self, 'id') and self.id is not None:
            return
        if hash_id:
            self.id = self._hashfile(os.path.join(self.dirpath, self.filename))
        elif self.data['title'] and self.data['artists']:
            header = u'TR-'
            string = u'title: {0}-artist: {1}'.format(self.data['title'],
                                                      '-'.join(
                                                          [artist['artist_name'] for artist in self.data['artists']]))
            self.id = header + string
        else:
            raise ValueError('If you are not using a hash, then you should call from_tags or from_kwargs first')
