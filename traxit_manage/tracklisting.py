"""
Created on dec. 29 2013

@author: Flavian
"""

import abc
import copy
import datetime
import json
import logging
import six
import time

import numpy as np

logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class Tracklisting(object):
    """Serves as a pseudo-abstract class for matching interfaces

    """

    def __init__(self, db=None, params=None):
        params = params or {}
        self.db = db
        self.history_matches = []
        self.history_tracklist = []
        for k, v in params.items():
            setattr(self, k, v)

    def reset(self):
        """Reset the cache inside the instance

        """
        self.history_matches = []
        self.history_tracklist = []

    # TODO(Deprecate end argument)
    def get_tracklist(self,
                      end,
                      tracklist_id=None):
        """Compute the tracklist.

        Available type_tracklist are:
          - detection or grountruth for a reporting compatible with PyAfe
          - web for a tracklist for the API
        """
        logger.warning('Argument ``end`` (here set to {0}) of get_tracklist will be deprecated'
                       .format(end))
        tracklist = self.compute_tracklist()
        assert (all((isinstance(tracklist_item['id'], basestring) or tracklist_item['id'] is None) and
                    'start' in tracklist_item and
                    'end' in tracklist_item
                    for tracklist_item in tracklist))
        tracklist = sorted(tracklist, key=lambda x: x['start'])
        tracklist_data = Tracklist(id=tracklist_id, tracklist=tracklist)
        logger.debug(u'Tracklist: {0}'.format(tracklist_data))
        return tracklist_data

    def times_list(self, end):
        """Get a list of time segments

        Given the end time of the media to analyze, return a list of windows.
        [(t1, t2), (t3, t4), ...]
        """
        t1 = 0
        t2 = 0
        return_list = []
        while t2 <= end:
            t1, t2 = self.pre_processing(t1, t2)
            return_list.append((t1, t2))
        return return_list


class Tracklist(object):
    """This object handles tracklist formatting, both from input and output perspectives.

    It also allows to investigate a tracklist content more easily by using the 'in' keyword.
    """
    tol = 0
    COMMON_DATETIME = datetime.datetime(2014, 1, 1)

    @staticmethod
    def from_groundtruth(d):
        """Inputs a representation fitted for PyAFE groundtruth files

        """
        tracklist = d['TrackList']['MusicTrack']
        for tracklist_item in tracklist:
            tracklist_item['start'] = int(tracklist_item['start'])
            tracklist_item['end'] = int(tracklist_item['end'])
            del tracklist_item['startDate']
            del tracklist_item['endDate']
        # If tracklist is an OrderedDict convert to dict
        tracklist = json.loads(json.dumps(tracklist, indent=4))
        return Tracklist(tracklist=tracklist)

    @staticmethod
    def from_detection(d):
        """Inputs a representation fitted for PyAFE detection files

        """
        d = d['submission']
        tracklist_id = d.get('submissionId', '')
        tracklist = d['detectionList']['MusicTrack']
        for tracklist_item in tracklist:
            del tracklist_item['eventDate']
        return Tracklist(id=tracklist_id, tracklist=tracklist)

    @staticmethod
    def from_dict(d):
        """Inputs an internal representation with id

        """
        tracklist_id = d['id']
        tracklist = d['tracklist']
        return Tracklist(id=tracklist_id, tracklist=tracklist)

    @staticmethod
    def sort_tracklist(tracklist):
        """Clean a tracklist object by removing None tracks and setting an "end" value.
        Args:
            tracklist (dict): Tracklist object defined in the swagger spec
        Returns:
            A cleaned tracklist object.
        """
        # Sort (start, end) in lexicographic order
        tracklist = sorted(tracklist,
                           key=lambda tracklist_segment: (tracklist_segment['start'],
                                                          tracklist_segment['end']))
        return tracklist

    def __init__(self, id=None, tracklist=None):
        """All fields are optional, but providing an id is better

        The clean boolean means that clean_tracklist will be called
        """
        self.id = id

        if tracklist is None:
            tracklist = []

        self.tracklist = self.sort_tracklist(tracklist)

    def __eq__(self, other):
        """Check for equality

        Args:
            other (traxit_algorithm.tracklisting.Tracklist): The other Tracklist instance this Tracklist instance
                should be compared to.

        Returns:
            bool: True or False
        """
        return self.tracklist == other.tracklist

    def contains(self, (track_id, start, end)):
        """Does the tracklist contain the tuple (track_id, start, end)?

        Be careful, if the tracklist was not aggregated (ie it contains
        {'id': '1', 'start': 0, end: 5} and {'id': '1', 'start': 5, end: 10})
        then items like ('1', 0, 10) will not be detected.

        Raises:
            ValueError: The segment is not contained into the tracklist's time extrema
        """
        track_in_tracklist = [start < track_segment['end'] < end or start <= track_segment['start'] < end or
                              track_segment['start'] <= start < track_segment['end']
                              for track_segment in self.tracklist]
        if not any(track_in_tracklist):
            raise ValueError('The segment is not contained into the tracklist\'s time extrema')
        indexes_in_tracklist = [index for index, in_tracklist in enumerate(track_in_tracklist)
                                if in_tracklist is True]
        detections = []
        for index_in_tracklist in indexes_in_tracklist:
            if track_id == self.tracklist[index_in_tracklist]['id']:
                detections.append(('TP', index_in_tracklist))
            elif self.tracklist[index_in_tracklist]['id'] is None:
                detections.append(('FAout', index_in_tracklist))
            else:
                detections.append(('FA', index_in_tracklist))
        return detections

    def __str__(self):
        """String representation of the Tracklist

        """
        return '<Tracklist - {id} - {nb} items>'.format(id=self.id,
                                                        nb=len(self.tracklist))

    def __repr__(self):
        """Returns a string that formats the tracklist.

        """
        logger.debug(u'Tracklist: {0}'.format(self.tracklist))

        if self.tracklist is None:
            logger.warning('Tracks are None')
            return '<tracklist not computed>'

        if not self.tracklist:
            logger.warning('Tracks are an empty list')
            return '<empty tracklist>'

        string = ''
        for tracklist_item in self.tracklist:
            track = tracklist_item['id']
            start = tracklist_item['start']
            try:
                if 'title' in track and track['contributors']:
                    string += u'{0} : Song: {1} / Artist: {2}\n' \
                        .format(time.strftime('%H:%M:%S', time.gmtime(start)),
                                track['title'],
                                track['contributors'][0]['name'])
                else:
                    string += u'{0} : TrackID: {1}\n' \
                        .format(time.strftime('%H:%M:%S', time.gmtime(start)),
                                str(track))
            except Exception as e:
                logger.error(u'{0} Could not show: {1}'
                             .format(time.strftime('%H:%M:%S',
                                                   time.gmtime(start)),
                                     e))
        return string

    def as_dict(self):
        """Exports the tracklist as a dict containing the id

        """
        logger.info(u'Exporting tracklist as dict')
        return {'tracklist': self.tracklist,
                'id': self.id}

    def as_groundtruth(self):
        """Outputs a representation fitted for PyAFE groundtruth files

        """
        logger.info(u'Exporting tracklist as groundtruth')
        tracklist_copy = copy.deepcopy(self.tracklist)
        for tracklist_item in tracklist_copy:
            tracklist_item.update({'startDate': str(self.COMMON_DATETIME +
                                                    datetime.timedelta(0, tracklist_item['start'])),
                                   'endDate': str(self.COMMON_DATETIME +
                                                  datetime.timedelta(0, tracklist_item['end'])),
                                   })
        tracklist_formated = {
            'TrackList': {
                'MusicTrack':
                    [tracklist_item
                     for tracklist_item in tracklist_copy]
            }
        }
        return tracklist_formated

    def as_detection(self):
        """Outputs a representation fitted for PyAFE detection files

        """
        logger.info(u'Exporting tracklist as detection')
        tracklist_copy = copy.deepcopy(self.tracklist)
        for tracklist_item in tracklist_copy:
            tracklist_item.update({'eventDate': str(self.COMMON_DATETIME +
                                                    datetime.timedelta(0, tracklist_item['start']))})
        tracklist_formated = {
            'submission': {
                'submissionId': self.id,
                'participantId': 'TraxIT',
                'detectionList': {
                    'MusicTrack': tracklist_copy
                }
            }
        }
        return tracklist_formated


class TracklistingV1(Tracklisting):
    """Our own Tracklisting algorithm

    """
    def __init__(self, db=None, params=None):
        """Initializes the Tracklisting instance

        """
        super(TracklistingV1, self).__init__(db, params)
        self.init_introspection()

    def init_introspection(self):
        """Initialize introspection

        """
        self.introspection = {'post_processing': {}}

    def pre_processing(self, t1=None, t2=None):
        """Get the new time segment from the previous one

        Args:
            t1 (float): Start of the segment to be updated. If None, then set to 0.
            t2 (float): End of the segment to be updated. If None then set according to t1.

        Returns:
            float, float: Updated time segment.
        """
        t1 = t1 or 0

        if not t2:
            return 0, self.processing_size

        return t1 + self.processing_hop, t2 + self.processing_hop

    def post_processing(self, match, t1, t2):
        """Applies some post processing to the matches

        Args:
            match:
            t1:
            t2:

        Returns:

        """
        self.init_introspection()
        if match.empty:
            self.history_matches.append((match, t1, t2))
        else:
            self.history_matches.append((match.sort(columns='score', ascending=False), t1, t2))
        if len(self.history_matches) == self.vote_horizon:
            t_start = self.history_matches[0][1]
            t_end = self.history_matches[-1][2]
            # Take the match with the best score
            matches = [_match.iloc[0] for _match, _, _ in self.history_matches if not _match.empty]
            logger.info('Post process from {0} to {1}'.format(t_start, t_end))
            # Post-processing: (track_id, start_th, shift, vote, m, score)
            res = []
            for match in matches:
                match = match.to_dict()
                if not res:
                    match['vote'] = 1
                    del match['p']
                    res = [match]
                else:
                    index_in_res = -1
                    for i, d in enumerate(res):
                        if (match['track_id'] == d['track_id'] and
                                np.abs(d['start_th'] - match['start_th']) < self.start_margin and
                                np.abs(match['shift'] - d['shift']) <= self.shift_margin):
                            index_in_res = i
                    if index_in_res != -1:
                        # Take for m the mean of the m's found
                        res[index_in_res]['m'] = (res[index_in_res]['vote'] * res[index_in_res]['m'] + match['m']) / (
                            res[index_in_res]['vote'] + 1)
                        res[index_in_res]['score'] = (res[index_in_res]['vote'] * res[index_in_res]['score'] + match[
                            'score']) / (res[index_in_res]['vote'] + 1)
                        res[index_in_res]['vote'] += 1
                        res[index_in_res]['end'] = match['end']
                    else:
                        match_res = copy.deepcopy(match)
                        match_res['vote'] = 1
                        del match_res['p']
                        res.append(match_res)
            res_threshold = [r for r in res if r['vote'] >= self.vote_threshold]
            # Sort by score
            res_threshold = sorted(res_threshold,
                                   key=lambda x: (x['vote'], x['score']),
                                   reverse=True)
            if res_threshold != []:
                # History: (track_id, start, end, start_th, shift, m, score)
                selected_match = copy.deepcopy(res_threshold[0])
                del selected_match['vote']
                self.history_tracklist.append(selected_match)
                # Introspection: (start, end, start_th, m, score)
                self.introspection['post_processing'].setdefault(res_threshold[0]['track_id'], [])
                self.introspection['post_processing'][res_threshold[0]['track_id']].append(res_threshold[0])
            else:
                self.history_tracklist.append({'track_id': None,
                                               'start': t_start,
                                               'end': t_end,
                                               'start_th': 0,
                                               'shift': 1,
                                               'm': 1,
                                               'score': 0})
            logger.debug('history_matches: {0}'.format(self.history_matches))
            del self.history_matches[0]

    def __repr__(self):
        """String representation for the Tracklisting instance

        """
        return 'Tracklisting V1'

    def compute_tracklist(self):
        """Computes a list of (id OR tracklist_item, start, end) from self.history and returns it.

        tracklist_item can be:
        dictionary{'id': unicode (MANDATORY),
                   'amp': amplitude in dB,
                   'pitch': pitch-shifting in % (int),
                   'stretch': time-stretching in % (int),
                   'eq': {'band0': x in dB,
                          'band1': x in dB,
                          'band2': x in dB},
                   'media_start': x (float)
                   'media_end': x (float)
                  }
        """
        if not self.history_tracklist:
            return []
        if len(self.history_tracklist) == 1:
            return [{'id': item['track_id'],
                     # 2 bins per note, 1 note = 6%
                     'pitch': -item['shift'] * 3,
                     # if m < 1 then the reference was sped up
                     'stretch': -(item['m'] - 1.) * 100,
                     'media_start': item['m'] * (item['start'] - item['start_th']),
                     'media_duration': item['m'] * (item['end'] - item['start']),
                     'start_th': item['start_th'],
                     'score': item['score'],
                     'start': item['start'],
                     'end': item['end']
                     }
                    for item in self.history_tracklist]
        # History: (track_id, start, end, start_th, shift, m)
        ########
        # This bit makes end[i] == start[i + 1]
        tracklist = []
        history = self.history_tracklist
        # Used to browse i and i + 1
        history_shifted = copy.deepcopy(history)
        del history_shifted[0]
        history_shifted.append(history_shifted[-1])
        for hist, hist_shift in zip(history, history_shifted):
            if hist['start'] == hist_shift['start'] and hist['end'] == hist_shift['end']:
                tracklist.append((hist['track_id'], hist['start'], hist['end'], hist['start_th'],
                                  hist['shift'], hist['m'], hist['score']))
            else:
                tracklist.append((hist['track_id'], hist['start'], hist_shift['start'], hist['start_th'],
                                  hist['shift'], hist['m'], hist['score']))
        tracklist_enhanced = [
            {
                'id': track_id,
                # 2 bins per note, 1 note = 6%
                'pitch': -shift * 3,
                # if m < 1 then the reference was sped up
                'stretch': -(m - 1.) * 100,
                'media_start': m * (_start - start_th),
                'media_duration': m * (_end - _start),
                'start_th': start_th,
                'score': score,
                'start': _start,
                'end': _end
            }
            for track_id, _start, _end, start_th, shift, m, score in tracklist]
        ###########
        return tracklist_enhanced
