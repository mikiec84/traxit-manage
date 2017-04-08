import errno
import os

def mkdir_p(path):
    """Equivalent of mkdir -p path

    Args:
        path: path of the directory to create

    """
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

audio_filetypes = [u'flac', u'm4a', u'mp3', u'ogg', u'wma', u'wav', u'mp4', u'aif', u'aiff', u'mp2']

DATA_FOLDER = os.environ.get('DATA_FOLDER', os.path.join(os.path.expanduser('~'), 'traxit_data_folder'))
AUDIO_REFERENCES_FOLDER = os.path.join(DATA_FOLDER, 'references')
WAVE_FOLDER = os.path.join(DATA_FOLDER, 'wave')
CSV_FOLDER = os.path.join(DATA_FOLDER, 'csv')

LOG_FOLDER = os.environ.get('LOG_FOLDER', '/var/log/traxit_core')

if not os.path.exists(DATA_FOLDER):
    mkdir_p(DATA_FOLDER)
if not os.path.exists(AUDIO_REFERENCES_FOLDER):
    mkdir_p(AUDIO_REFERENCES_FOLDER)
if not os.path.exists(WAVE_FOLDER):
    mkdir_p(WAVE_FOLDER)
if not os.path.exists(CSV_FOLDER):
    mkdir_p(CSV_FOLDER)
if not os.path.exists(LOG_FOLDER):
    mkdir_p(LOG_FOLDER)

working_directories_file = os.path.join(DATA_FOLDER, "corpus.json")
