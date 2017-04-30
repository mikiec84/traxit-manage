import logging

import numpy as np
import pandas as pd
from tracklisting import Tracklist, Tracklisting

logger = logging.getLogger(__name__)


class SampleFingerprinting(object):
    """This class is a very simple fingerprinting algorithm.

    Args:
        params (dict): Dictionary of parameters which will be passed into self attributes.
    """

    def __init__(self, params=None):
        if params is None:
            params = {}
        self.sr = 11025
        self.algo_name = "SampleFingerprinting"
        for k, v in params.items():
            setattr(self, k, v)

    def get_fingerprint(self, audio, t1=0, t2=None, post_process=False):
        """Returns the fingerprint for the given audio signal.

        The simplest fingerprint is the signal itself.

        Args:
            audio (np.array): a 1-D numpy array of floats between -1 and 1 that is already an extract between t1 and t2.
            t1 (Optional[int]): Start time of the fingerprinting process in the audio. Default to 0.
            t2 (Optional[int or None]): End time of the fingerprinting process in the audio. Defaults to None
            post_process (Optional[bool]): Apply post processing or not

        Returns:
            pandas.DataFrame: Computed fingerprint.
        """
        fp = pd.DataFrame({'key': audio[::50]})

        return fp

    def how_much_audio(self, start, end):
        """Compute how much audio, in buffer units, is needed to compute a fingerprint between ``start`` and ``end``.

        Args:
            start (float): start time in seconds.
            end (float): end time in seconds.

        Returns:
            tuple of (int, int): Start buffer index, end buffer index.
        """
        return int(start * self.sr), int(np.ceil(end * self.sr))


class SampleMatching(object):
    """Simplest matching: check if two fingerprints are strictly equal.

    You have access to self.db, a database instance, and self.fingerprinting
    """

    def __init__(self, db, params=None, fingerprinting=None):
        """Initializes the Fingerprinting class.

        Args:
            db: an instance of a child of DbApi

            params: a dictionary of parameters.
            Corresponds to params['fingerprint']

            fingerprinting: an instance of a child of Fingerprinting.
        """
        params = params or {}

        if db is None:
            logger.warning('Be careful, you are instanciating a matching without db')
        self.db = db
        self.fingerprinting = fingerprinting

    def get_matches(self, fp, t1, t2, introspect_trackids=None, query_keys_n_jobs=1):
        """Improvement over the simple ``get_candidates`` and ``get_scores`` pipeline.

        Args:
            fp: fingerprint to match
            t1: start of the fingerprint segment to process
            t2: end of the fingerprint segment to process
            introspect_trackids: useless here
            query_keys_n_jobs (int): how many jobs should be started by joblib

        Returns:
            pandas.DataFrame: A dataframe with columns: 'track_id', 'score'
        """
        tracks_scored_unsorted = []
        track_ids = self.db.query_track_ids(fp.key, 10)
        all_queried_keys = self.db.query_keys(fp.key, track_ids)

        for track_id in track_ids:
            queried_keys = all_queried_keys[track_id]

            score = len(queried_keys)

            tracks_scored_unsorted.append({
                'track_id': track_id,
                'score': score,
            })
        tracks_scored = sorted(tracks_scored_unsorted, key=lambda x: x['score'], reverse=True)
        tracks_scored = pd.DataFrame(tracks_scored)

        return tracks_scored


class SampleTracklisting(Tracklisting):
    """Simplest possible tracklisting algorithm

    """
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
