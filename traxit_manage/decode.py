import logging
import wave

import numpy as np

logger = logging.getLogger(__name__)

traxit_decode = False

try:
    from traxit_decode.decode import Decode, decode_wave, length_wave
    traxit_decode = True
except ImportError:
    logger.warning('traxit_decode library not present, reverting to a simpler version where only wave files can be used.')
    traxit_decode = False


if not traxit_decode:
    class Decode(object):
        """Decode an input audio file in a 16 bit wav file at a specific rate of 11025Hz.

        Args:
            location (unicode): Path to the file to reade.
            mode (str): only 'filewavsink'
            location_store (unicode): Path to the file to write to

        Raises:
            ValueError: In memory read/writes are available only in traxit_decode
        """

        def __init__(self,
                     location,
                     mode='filewavsink',
                     location_store=None,
                     url=False,
                     **kwargs):
            self._data = None
            if mode != 'filewavsink':
                raise ValueError('In memory read/writes are available only with the traxit_decode library')
            if location_store is None:
                raise ValueError('In memory read/writes are available only with the traxit_decode library')
            if url:
                raise ValueError('Reading from an URL is only available with the traxit_decode library')
            self.location = location
            self.location_store = location_store
            self.sr = 11025

        def start(self):
            """Start decoding."""
            from scipy.signal import decimate
            from scipy.io import wavfile

            rate, data = wavfile.read(self.location)

            assert data.dtype == np.int16

            nchans = data.shape[1]

            # If there are more than one channel, take the mean to have a mono signal
            if nchans > 1:
                data = data.astype(np.int32)
                data = np.sum(data, axis=1) / nchans

            # Resample
            new_rate = 11025
            q = rate / new_rate
            if q * new_rate != rate:
                raise ValueError('The rate of your input file at {} is {} but '
                                 'must be a multiple of {}'.format(self.location, rate))

            # Change type for resampling calculation
            data = data.astype(np.float64)
            resampled = decimate(data, q, zero_phase=True)
            # Go back to 16 bit
            resampled = resampled.astype(np.int16)

            self._data = resampled

            wavfile.write(self.location_store, new_rate, resampled)


        def get_data(self, t0=0, t1=None):
            """Get data between two timestamps.

            Args:
                t0 (float): Start point in seconds
                t1 (float): End point in seconds

            Returns:
                np.array: Buffer of integers

            Raises:
                NotImplementedError: Only available in the traxit_decode library
            """
            return self.get_raw_data(11025 * start, 11025 * end)

        def get_raw_data(self, start=0, end=None):
            """Get data between two timestamps.

            Args:
                start (int): Start point in buffer unit
                end (int): End point in buffer unit

            Returns:
                np.array: Buffer of integers

            Raises:
                NotImplementedError: Only available in the traxit_decode library
            """
            return self._data[start:end]



    def decode_wave(infilename, buf_start=0, buf_end=None, samplerate_assert=None):
        """Decode wave files between frame buf_start and buf_end (not included).

        Args:
            infilename: Filename to decode
            buf_start (int): Decoding start in buffer unit. Defaults to 0
            buf_end (int or None): Decoding end in buffer unit. Decode until the end it None (default)
            samplerate_assert (int or None): Assert the sample rate takes a specific value. Do not assert if None (default)
        """
        infile = wave.open(infilename, "rb")
        width = infile.getsampwidth()
        rate = infile.getframerate()
        n_frames = infile.getnframes()
        buf_end = buf_end if buf_end is not None else n_frames
        if samplerate_assert is not None:
            assert rate == samplerate_assert
        assert width == 2, "Invalid wave: width must by 2 bytes"
        if buf_end > n_frames:
            logger.warning(u"buf_end must be lower than number of frames. Setting it to max frame.")
            buf_end = n_frames
        if buf_start >= n_frames:
            logger.warning(u"empty buffer")
            return []
        if buf_start < 0:
            buf_start = 0
        length = buf_end - buf_start
        anchor = infile.tell()
        infile.setpos(anchor + buf_start)
        data = np.fromstring(infile.readframes(length), dtype=np.int16)
        infile.close()
        return data, buf_end == n_frames


    def length_wave(infilename):
        """Returns the length in seconds of a wave file.

        Args:
            infilename: Filename to decode

        Returns:
            float: Length in seconds
        """
        infile = wave.open(infilename, "rb")
        rate = infile.getframerate()
        n_frames = infile.getnframes()
        return float(n_frames) / rate
