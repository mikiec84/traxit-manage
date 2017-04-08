"""
Created on 18 fevr. 2015

@author: Flavian
"""

import base64
import copy
import csv
import glob
import hashlib
import json
import logging
import os

import numpy as np
import xmltodict

logger = logging.getLogger(__name__)


class NumpyEncoder(json.JSONEncoder):
    """Encoder for the json library for encoding numpy arrays.

    """

    def default(self, obj):
        """If input object is an ndarray, converts it into a dict holding dtype, shape and the data base64 encoded.

        Args:
            obj: Object to encode

        Returns:
            str: Encoding the object
        """
        if isinstance(obj, np.ndarray):
            data_b64 = base64.b64encode(np.ascontiguousarray(obj).data)
            return dict(__ndarray__=data_b64,
                        dtype=str(obj.dtype),
                        shape=obj.shape)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder(self, obj)


def json_numpy_obj_hook(dct):
    """Decodes a previously encoded numpy ndarray with proper shape and dtype

    Args:
        dct (dict): JSON-encoded ndarray

    Returns:
        ndarray: If input was an encoded ndarray

    """
    if isinstance(dct, dict) and '__ndarray__' in dct:
        data = base64.b64decode(dct['__ndarray__'])
        return np.frombuffer(data, dct['dtype']).reshape(dct['shape'])
    return dct


# Overload dump/load to default use this behavior.
def json_dumps(*args, **kwargs):
    """Wrapper on json.dumps

    """
    kwargs.setdefault('cls', NumpyEncoder)
    return json.dumps(*args, **kwargs)


def json_dump(*args, **kwargs):
    """Wrapper on json.dump

    """
    kwargs.setdefault('cls', NumpyEncoder)
    return json.dump(*args, **kwargs)


def json_loads(*args, **kwargs):
    """Wrapper on json.loads

    """
    kwargs.setdefault('object_hook', json_numpy_obj_hook)
    return json.loads(*args, **kwargs)


def json_load(*args, **kwargs):
    """Wrapper on json.load

    """
    kwargs.setdefault('object_hook', json_numpy_obj_hook)
    return json.load(*args, **kwargs)


def hash_fingerprint_params(params):
    m = hashlib.sha256()
    params_fingerprint = params.get('fingerprint', {})
    params_string = ''.join(sorted([str(value) for value in params_fingerprint.values()]))
    m.update(base64.b64encode(params_string))
    hash_params = m.hexdigest()[:10]
    return hash_params


def make_db_name(corpus,
                 broadcast=None,
                 nb=None,
                 algo_name=None,
                 params=None):
    """Construct a db name from the identification characteristics"""
    if params is None:
        from traxit_algorithm import parameters
        params = parameters.params
    if algo_name is None:
        algo_name = 'traxittest'  # There is an index template associated to this prefix
    if nb is not None:
        nb = str(nb)
    hash_params = hash_fingerprint_params(params)
    array_names = [algo_name, corpus, broadcast, hash_params, nb]
    db_name = '_'.join(filter(None, array_names))
    return db_name


def insensitive_glob(pattern):
    """Performs a glob.glob operation insensitive to case.

    Args:
        pattern: argument of glob.glob

    Returns:
        a list of paths
    """

    def either(c):
        return '[{0}{1}]'.format(c.lower(), c.upper()) if c.isalpha() else c

    return glob.glob(''.join(map(either, pattern)))


def dict_to_xml(d):
    """Transforms a dictionary into an XML string.

    Args:
        d: dictionary

    Returns:
        XML string
    """
    return xmltodict.unparse(d, pretty=True)


def xml_to_dict(filename='', string=''):
    """Transforms an XML file or an XML string into a dictionary.

    In case there is an attribute in the XML tag, creates an _attribute key
    in the dictionary mimicking the current structure

    Args:
        filename (string): path of an XML file to open. If set, string is not read.
        string (string): XML string read if filename is an empty string.

    Returns:
        dicionary following the structure of the XML file or string.
    """
    if filename != '':
        with open(filename, 'r') as myfile:
            string = myfile.read()
    return xmltodict.parse(string)


def clean_list_of_files(list_of_files):
    """Clean a list of files by checking if their path exists

    Args:
        list_of_files: a list or a single string

    Returns:
        A list of files that exist on the file system
    """
    list_of_files = ([list_of_files]
                     if not isinstance(list_of_files, list)
                     else list_of_files)
    list_of_files = [filename for filename in list_of_files
                     if os.path.exists(filename)]
    if list_of_files == []:
        logger.warning(u'List of files is empty!')
    return list_of_files


def get_audio_files(path, audio_filetypes):
    """Get all audio files in a folder, given a list of types.

    Args:
        path (unicode): path of the directory where the files can be found
        audio_filetypes: list of string extensions of audio files (without '.'). Example: ['mp3', 'mp4']
    """
    audio_files = []
    for audio_type in audio_filetypes:
        regex_path = os.path.join(path, u'*.' + audio_type)
        audio_files_type = insensitive_glob(regex_path)
        audio_files.extend(audio_files_type)
    return audio_files


def split_dir_file_ext(path):
    """Separates a path as directory name, file name, file extension.

    Args:
        path (string): the path to split

    Returns:
       tuple (dir_name, file_name, file_extension)

    Examples:
        >>> split_dir_file_ext('/path/to/somefile.ext')
        ('/path/to', 'somefile', 'ext')

        >>> split_dir_file_ext('/path/to/somefile.tar.gz')
        ('/path/to', 'somefile.tar', 'gz')

        >>> split_dir_file_ext('/path/to/somefile')
        ('/path/to', 'somefile', '')
    """
    dir_name = os.path.dirname(path)
    file_name_complete = os.path.basename(path)
    file_name, file_extension = os.path.splitext(file_name_complete)
    file_extension = file_extension[1:]
    return dir_name, file_name, file_extension


def listdict2csv(csv_file,
                 listdict,
                 delimiter=';',
                 quoting=csv.QUOTE_MINIMAL,
                 quotechar='|'):
    """Serializes a list of dictionaries to CSV

    Args:
        csv_file: file-like
        listdict: one or a list of dictionaries with the same keys, and json-serializable values
    """
    c = csv.writer(csv_file,
                   quoting=quoting,
                   delimiter=delimiter,
                   quotechar=quotechar)
    if not isinstance(listdict, list):
        assert isinstance(listdict, dict)
        logger.warning(u'Bare dictionary sent to listdict2csv, changing it to [listdict]')
        listdict = [listdict]
    c.writerow(listdict[0].keys())
    for d in listdict:
        c.writerow([json.dumps(d[key]) for key in d])


def csv2listdict(csv_file, delimiter=';',
                 quoting=csv.QUOTE_MINIMAL,
                 quotechar='|'):
    """Deserializes a CSV into a list of dictionaries. Columns of the CSV must be json-deserializable.

    Args:
        csv_file: file-like

    Returns: a list of dictionaries
    """
    headers = None

    d = []
    reader = csv.reader(csv_file,
                        quoting=quoting,
                        delimiter=delimiter,
                        quotechar=quotechar)
    for row in reader:
        if reader.line_num == 1:
            headers = row
        else:
            sub_dict = dict(zip(headers, row))
            sub_dict_decoded = decode_sub_dict(sub_dict)
            d.append(sub_dict_decoded)
    return d


def decode_sub_dict(sub_dict):
    """Decode a sub dictionary from a CSV read and assign the right types to the values.

    Args:
        sub_dict: a sub directory

    Returns:
        a sub directory similar to the input one but where '1' becomes 1, json values become dicitonaries etc.
    """
    sub_dict_decoded = copy.deepcopy(sub_dict)
    try:
        sub_dict_decoded = {key: json.loads(value)
                            for key, value in sub_dict.iteritems()}
    except ValueError:
        for key in sub_dict:
            isnumeric = False
            try:
                sub_dict_decoded[key] = int(sub_dict[key])
                isnumeric = True
            except ValueError:
                logger.info('Could not cast to int')
            try:
                if not isnumeric:
                    sub_dict_decoded[key] = float(sub_dict[key])
            except ValueError:
                logger.info('Could not cast to float')
    return sub_dict_decoded


def get_tracklist_from_csv(corpus_path, csv_path):
    from traxit_algorithm.track import Track
    from traxit_algorithm.tracklisting import Tracklist

    references = read_references(corpus_path)
    with open(csv_path, 'rU') as csv_file:
        list_dict = csv2listdict(csv_file)
    assert len(list_dict) > 0, 'Your file is empty.'
    assert 'filename' in list_dict[0], ('Your file should have a \'filename\' '
                                        'header. Current header: {0}'.format(list_dict[0]))
    assert 'start' in list_dict[0], ('Your file should have a \'start\' '
                                     'header. Current header: {0}'.format(list_dict[0]))
    assert 'end' in list_dict[0], ('Your file should have a \'end\' '
                                   'header. Current header: {0}'.format(list_dict[0]))
    references_broadcast = {}
    for item in list_dict:
        filename = item['filename']
        if filename not in references:
            IOError('File {0} is not in the references directory.'.format(filename))
        references_broadcast[filename] = references[filename]

    filename_start_end = []
    filename_song = {}

    for el in list_dict:
        filename_start_end.append((unicode(el['filename']),
                                   int(el['start']),
                                   int(el['end'])))
        filename_song[unicode(el['filename'])] = Track(
            filepath=os.path.join(corpus_path,
                                  'references',
                                  unicode(el['filename'])
                                  ),
            generate_id=True
        )
        filename_song[unicode(el['filename'])].from_tags()

    tracklist = []
    for filename, start, end in filename_start_end:
        track = filename_song[filename].as_dict()
        track.update({'start': start, 'end': end})
        tracklist.append(track)
    tl = Tracklist(tracklist=tracklist)
    return tl, references_broadcast


def query_yes_no(question, default='yes'):
    """Ask a yes/no question via raw_input() and return their answer.

    Args:
        question (string): question which is asked
        default: the presumed answer if the user just hits <Enter>. It must
                 be one of "yes" (default), "no" or None (meaning the user must enter a string).
    Returns:
        a boolean True or False for "yes" or "no"
    """
    valid = {'yes': True,
             'y': True,
             'ye': True,
             'no': False,
             'n': False}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError('invalid default answer: {0}' .format(default))

    while True:
        print(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print('Please respond with "yes" or "no" '
                  '(or "y" or "n").\n')


def path_corpus(corpus):
    """Retrieves the path to a corpus from the corpus name

    Args:
        corpus (string): corpus to resolve

    Returns:
        a path string
    """
    corpora = read_corpus()

    if corpus not in corpora:
        raise ValueError('The corpus you stated does not exist.')
    return unicode(corpora[corpus])


def get_corpus_broadcast(corpus, broadcast=None, broadcast_alias=None):
    """Returns corpus, corpus_path, broadcast, broadcast_alias, broadcast_path as unicode strings

    """
    if broadcast_alias is None:
        broadcast_alias = broadcast
    corpus_path = path_corpus(corpus)
    if broadcast is not None and not is_broadcast(corpus_path, broadcast):
        raise ValueError(u'{0} is not a valid broadcast!'.format(broadcast))
    else:
        broadcast_path = os.path.join(corpus_path, broadcast) if broadcast is not None else None
    if broadcast_alias is not None and not is_broadcast(corpus_path, broadcast_alias):
        raise ValueError(u'{0} is not a valid broadcast alias!'.format(broadcast_alias))
    return corpus, corpus_path, broadcast, broadcast_alias, broadcast_path


def is_broadcast(corpus_path, broadcast):
    """Check if a broadcast is valid

    Args:
        corpus_path (str): path to a corpus directory
        broadcast (str): broadcast to check

    Returns:
        bool: True if exists else False

    """
    return os.path.exists(os.path.join(corpus_path,
                                       broadcast,
                                       'groundtruth.xml'))


def read_corpus():
    """Read the corpus names and locations

    Returns:
        dict: corpus: corpus_path
    """
    from traxit_manage import settings
    if not os.path.exists(settings.working_directories_file):
        write_corpus({})
    with open(settings.working_directories_file, 'rb') as f:
        return json.load(f)


def write_corpus(corpus_dict):
    """Write a new corpus to the config.json file

    Args:
        corpus_dict (dict): the corpora
    """
    from traxit_manage import settings
    with open(settings.working_directories_file, 'wb+') as f:
        json.dump(corpus_dict, f, indent=4)


def file_path(corpus_path, broadcast, suffix, filename):
    if suffix is not None:
        suffix = unicode(suffix)
    filename = '_'.join(filter(None, [filename, suffix])) + '.json'
    if broadcast is not None:
        path = os.path.join(corpus_path, broadcast, filename)
    else:
        path = os.path.join(corpus_path, filename)
    return path


def read_references(corpus_path, broadcast=None):
    """Reads the content of references.json in a corpus or broadcast.

    Args:
        corpus_path: path to a corpus directory
        broadcast: broadcast to check

    Returns:
        dict: Dictionary of filepath -> filename

    Raises:
        No references file exists.
    """
    path = file_path(corpus_path, broadcast, '', 'references')
    try:
        if os.path.exists(path):
            with open(path, 'rb') as f:
                references = json.load(f)
                assert isinstance(references, dict)
                return references
        else:
            raise IOError('No references file exists.')
    except ValueError:
        return {}


def write_references(references,
                     corpus_path,
                     broadcast=None):
    """Writes the content of references file in a corpus or broadcast.

    A references file can also be named after a number and a datetime.datetime

    Args:
        references: a dictionary of references
        corpus_path: path to a corpus directory
        broadcast: broadcast to check
    """
    assert isinstance(references, dict)
    path = file_path(corpus_path, broadcast, '', 'references')
    with open(path, 'wb+') as f:
        json.dump(references, f)


def get_audio_files_not_cached(path, audio_filetypes, audio_cache_filetype):
    """Get all audio files in a folder, given a list of types.

    Ignores cached files, which are in the form of `track.mp3.wav` (which caches `track.mp3`)

    Args:
        path (unicode): path of the directory where the files can be found
        audio_filetypes: list of string file types
        audio_cache_filetype: extension of the cache files

    Returns:
        a list of audio files which are not cached
    """
    audio_files = get_audio_files(path, audio_filetypes)
    audio_files_not_cached = [audio_file
                              for audio_file in audio_files
                              if audio_cache_filetype != split_dir_file_ext(audio_file)[2]]
    return audio_files_not_cached
