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

            score = sum(len(value) for key, value in queried_keys.iteritems())

            tracks_scored_unsorted.append({
                'track_id': track_id,
                'score': score,
            })
        tracks_scored = pd.DataFrame(tracks_scored_unsorted).sort_values('score', ascending=False)

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
        """Update self.history_tracklist (Note: history_tracklist must be json-serializable)

        Args:
            match:
            t1:
            t2:

        Returns:

        """
        logger.info('Adding match {0} at {1}'.format(match, t1))
        self.history_tracklist.append(match.to_dict())

    def __repr__(self):
        """String representation for the Tracklisting instance

        """
        return 'SampleTracklisting'

    def compute_tracklist(self):
        """Computes a list of (id OR tracklist_item, start, end) from self.history_tracklist and returns it.

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
        aggregation = pd.concat([pd.DataFrame.from_dict(match) for match in self.history_tracklist])
        if not aggregation.empty:
            logger.info('Aggregation: {0}'.format(aggregation))
            best_matches = (aggregation
                            .groupby(by='track_id')['score']
                            .sum()
                            .sort_values(ascending=False)
                            .head(10)
                            )
            logger.info('Best matches: {0}'.format(best_matches))
            best_match = best_matches.head(1)
            return [{
                'id': best_match.index[0],
                'start': 0,
                'end': 200,
                'score': best_match.values[0]
            }]
