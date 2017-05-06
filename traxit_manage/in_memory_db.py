import logging
import os
import shutil

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)

class DbInMemory:
    """In memory fingerprint database."""
    def __init__(self, db_name):
        self.store_in = os.path.join('/tmp', db_name)
        if not os.path.exists(self.store_in):
            os.mkdir(self.store_in)
            self._fps = None
        else:  # Load existing fingerprints
            fp_files = [f for f in os.listdir(self.store_in) if os.path.isfile(os.path.join(self.store_in, f))]
            fps = []
            for fp_file in fp_files:
                fp = pd.read_csv(os.path.join(self.store_in, fp_file))
                fp['track_id'] = fp_file
                fps.append(fp)
            if fps:
                self._fps = pd.concat(fps)


    def __repr__(self):
        """Representation of the in memory DB"""
        return self.__str__()

    def __str__(self):
        """Representation of the in memory DB"""
        return 'In memory database persisted in directory {0}'.format(self.store_in)

    def keys_count(self):
        """Counts the number of distinct track ids in the database.

        Returns:
            int: Total number of keys
        """
        return len(self._fps.track_id.unique())


    def insert_fingerprint(self, fp, track_id, override=False):
        """Inserts a fingerprint into the in memory database.

        If a fingerprint with a given track_id exists, it is replaced if override is True.
        If one exists but is partial (because the insert failed half way for some reason),
        then it will be completely replaced.

        Args:
            fp: A pandas dataframes with a column named `key`.
            track_id: The id referencing the fingerprint.
            override: Boolean to replace a previously existing fingerprint.
        """
        if not isinstance(fp, pd.DataFrame):
            raise TypeError('fp must be a pandas dataframe')

        logger.info('Making a copy of the fingerprint.')
        fp = fp.copy()

        logger.info('Constructing the documents.')

        # Setting index_ref as a column equal to the index
        fp['index_ref'] = fp.index

        fp.to_csv(os.path.join(self.store_in, track_id))

        fp['track_id'] = track_id

        if self._fps is None:
            self._fps = fp
        else:
            self._fps = pd.concat((self._fps, fp))

    def query_keys(self, keys, track_ids):
        """Query keys from in memory db

        Args:
            keys (set of int): Keys to be used to search for the tracks.
            track_ids (iterator of str): Iterator of track IDs to that correspond best to the queried keys.

        Results:
            dict: {track_id: {key: [{"index": np.array}, ...] }}
        """
        result = {}

        subfps = self._fps[self._fps.track_id.isin(track_ids) & self._fps.key.isin(keys)]
        subfps = subfps.rename(columns={'index_ref': 'index'})
        for track_id, track_id_group in subfps.groupby('track_id'):
            group = track_id_group.groupby('key')['index'].apply(np.array).to_frame('index')

            result[track_id] = group.to_dict('index')

        return result

    def query_track_ids(self, keys, size, quality=5):
        """Query track_ids from a set of integer keys

        Args:
            keys (set of int): Keys to be used to search for the tracks.
            size (int): Number of tracks to return as a result.
            quality (Optional[int]): Positive number (greater than 0). This will be passed to Elasticsearch as
                `size_shard = size * quality`. Defaults to 5.

        Returns:
            list of str: Ordered list of track IDs to that correspond best to the queried keys. Ordered from most
                relevant to less relevant.
        """
        if self._fps is None:
            return []
        query = self._fps[self._fps.key.isin(keys)]
        count = query.groupby('track_id')['track_id'].count()
        count.sort_values(ascending=False, inplace=True)
        return count.index[:size].tolist()

    def query_fingerprint(self, track_id=None, return_fields=None):
        """Query a fingerprint"""
        if self._fps is None:
            return None

        fp = self._fps[self._fps.track_id == track_id]
        fp = fp.drop(['track_id', 'index_ref'], 1)

        return fp


    def get_fp_ids(self, offset=0, size=10):
        """Get track IDs for fingerprints

        Args:
            offset (Optional[int]): Where to begin in the whole list of IDs. Defaults to 0.
            size (Optional[int]): How many track IDs  to query

        Returns:
            list of str: List of track IDs
        """
        if self._fps is None:
            return []

        return self._fps.track_id.unique()


    def is_ingested_fingerprint(self, track_id, **kwargs):
        """Checks if the fingerprint was already ingested

        Args:
            track_id: id of the fingerprint

        Returns:
            A boolean which is True if the fingerprint was ingested, False otherwise

        Raises:
           ValueError if check_partial is True and a partial fingerprint was found
        """
        if self._fps is None:
            return False

        return (track_id in self._fps.track_id.values)


    def delete_key(self, key):
        """Delete one key"""

    def delete_fingerprint(self, track_id):
        """Delete one fingerprint"""
        self._fps = self._fps[self._fps.track_id != track_id]

    def delete_all(self):
        """Deletes data but not the index. If you change the mapping then it will not update with a delete_all query."""
        if os.path.exists(self.store_in):
            shutil.rmtree(self.store_in)
            os.mkdir(self.store_in)
        self._fps = None
