#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on dec. 28 2013

@author: Flavian
"""

import logging
from logging.handlers import RotatingFileHandler
import os

import click

FORMAT = '%(asctime)-15s %(levelname)s %(name)s %(message)s'

time_format = '%Y-%m-%dT%H:%M:%S'


@click.group()
def main():
    click.echo(click.style(u'\n\n              T  R  A  X    I  T  \u00A9', fg='yellow'))
    click.echo("""
                _________________
               /                 \\
               \\                 /
                \\      ___      /
                 \\    /   \\    /
                  \\  /     \\  /
                   \\/       \\/
                   /\\       /\\
                  /  \\     /  \\
                 /    \\___/    \\
                /               \\
               /                 \\
               \\_________________/
""")
    click.echo(click.style('\n\n   C O M M A N D   L I N E  I N T E R F A C E\n\n\n', fg='yellow'))
    click.echo(u'               T  R  A  X' + click.style(u'    A  I  R\n\n', fg='blue'))
    logger = logging.getLogger()
    logger.level = logging.INFO

    from traxit_manage import settings
    file_handler = RotatingFileHandler(os.path.join(settings.LOG_FOLDER, 'cli.log'),
                                       maxBytes=10000000,
                                       backupCount=3)
    file_handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(file_handler)


@main.command()
@click.argument('corpus')
@click.argument('broadcast', default='', required=False)
@click.option('--dbname', help='Database name. If not set, the name of the database will be chosen according '
                               'to the algorithm name and parameters.')
@click.option('--erase', is_flag=True, help='Erase the database.')
def ingest(corpus, broadcast, dbname, erase):
    """Creates a list of references that the broadcast will be compared to.

    By default the database wrapper used is DbElastic
    """
    from traxit_manage.ingest import ingest_references
    res = ingest_references(corpus=corpus,
                            broadcast=broadcast,
                            erase=erase,
                            db_name=dbname,
                            cli=True)
    if res:
        list_of_files, list_of_valid = res
        click.echo(u"""
        Number of input files: {0}
        Number of input files that were valid: {1}
        )
         """.format(len(list_of_files),
                    len(list_of_valid)))


@main.command()
@click.argument('corpus')
@click.argument('broadcast')
@click.option('--dbname', help='DB name. If not set, the name of the database will be chosen according to the algorithm name and parameters.')
@click.option('--reset-cache', is_flag=True, help='Removes cache for wave and tracklist history')
@click.option('--reset-history-tracklist', is_flag=True, help='Removes cache for tracklist history')
@click.option('--globaldb', is_flag=True, help='Use the corpus database.')
@click.option('--pipeline', help='Pipeline name to use from traxit_algorithm.pipeline.')
@click.option('--introspect-trackids', help='Comma-separated track_ids to introspect. Result will be stored in introspect.json')
def tracklist(corpus, broadcast, dbname,
              reset_cache, reset_history_tracklist, globaldb, pipeline, introspect_trackids):
    """Tracklists according to a list of references that have been previously ingested

    By default the database wrapper used is DbElastic
    """
    from traxit_manage.tracklist import tracklist_helper
    detection_file_append = ''
    if introspect_trackids:
        introspect_trackids = introspect_trackids.split(',')
    tracklist_helper(corpus=corpus,
                     broadcast=broadcast,
                     reset_cache=reset_cache,
                     reset_history_tracklist=reset_history_tracklist,
                     globaldb=globaldb,
                     db_name=dbname,
                     cli=True,
                     pipeline=pipeline,
                     detection_file_append=detection_file_append,
                     introspect_trackids=introspect_trackids)


@main.command()
@click.argument('corpus')
def init_corpus(corpus):
    """Inits a corpus in the current directory. Delete it with delete_corpus

    """
    from traxit_manage.corpus import init_corpus_helper
    corpus_path = os.getcwdu()
    init_corpus_helper(corpus_path, corpus)


@main.command()
def list_corpus():
    """List available corpora

    """
    from traxit_manage.corpus import list_corpus_helper
    corpora = list_corpus_helper()
    for key, value in corpora.iteritems():
        click.echo(u'{0}: {1}'.format(key, value))


@main.command()
@click.argument('corpus')
@click.option('--dry-run', is_flag=True, help='Only show which files would be erased without erasing them')
def clean_references(corpus, dry_run):
    """Delete references with missing tags

    """
    from traxit_manage.corpus import clean_references_helper
    deleted = clean_references_helper(corpus, dry_run)
    if deleted:
        if dry_run:
            click.echo('Files set to be deleted:')
        else:
            click.echo('Deleted files:')
        for item in deleted:
            click.echo(u'{filepath}: {reason}'.format(filepath=item,
                                                      reason=deleted[item]))
    else:
        click.echo('No file set to be deleted.')


@main.command()
@click.argument('corpus', default=None, required=False)
@click.option('--force', is_flag=True, help='')
def delete_corpus(corpus, force):
    """Delete corpus

    """
    from traxit_manage.corpus import delete_corpus_helper
    delete_corpus_helper(corpus, force)


@main.command()
@click.argument('corpus')
@click.argument('broadcast')
def init_broadcast(corpus, broadcast):
    """Init a broadcast

    """
    from traxit_manage.broadcast import init_broadcast_helper
    corpus_path, broadcast = init_broadcast_helper(broadcast, corpus)
    click.echo(u'Broadcast {0} created in {1}'.format(broadcast,
                                                      corpus_path))


@main.command()
@click.argument('corpus')
def list_broadcast(corpus):
    """List broadcasts in a corpus

    """
    from traxit_manage.broadcast import list_broadcast_helper
    broadcast_dict = list_broadcast_helper(corpus)
    for broadcast_name, broadcast in broadcast_dict.iteritems():
        print('{0}: {1}'.format(broadcast_name, broadcast))


@main.command()
@click.argument('corpus')
@click.argument('broadcast')
@click.argument('filename')
def generate_from_broadcast(corpus, broadcast, filename):
    """Populates groundtruth in an initiated broadcast with data from a csv file.

    """
    from traxit_manage.broadcast import generate_from_broadcast_helper
    split = filename.split(os.extsep, 1)
    if split[1] == 'csv':
        try:
            generate_from_broadcast_helper(corpus, broadcast, filename)
        except ValueError as e:
            print(e)
    else:
        print('The type of your file should be csv.')


@main.command()
@click.argument('corpus')
@click.argument('broadcast')
def delete_broadcast(corpus, broadcast):
    """Delete a broadcast

    """
    from traxit_manage.broadcast import delete_broadcast_helper
    delete_broadcast_helper(broadcast, corpus)
