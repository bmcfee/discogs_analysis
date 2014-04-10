#!/usr/bin/env python

import argparse
import os
import sys
import time

import cPickle as pickle

import couchdb

import whoosh
import whoosh.analysis
import whoosh.index
import whoosh.fields

from whoosh.support.charset import accent_map

def duration_to_int(v):

    try:
        tstr = time.strptime(v, '%H:%M:%S')
    except ValueError:
        try:
            tstr = time.strptime(v, '%M:%S')
        except ValueError:
            try:
                tstr = time.strptime(v, '%S')
            except ValueError:
                return 0

    return int(tstr.tm_hour * 60 * 60 + tstr.tm_min * 60 + tstr.tm_sec)

def create_index_writer(index_path):
    '''Create a new whoosh index in the given directory path.
    
    Input: directory in which to create the index
    
    Output: `whoosh.index` writer object
    '''
    

    if not os.path.exists(index_path):
        os.mkdir(index_path)

    analyzer = (whoosh.analysis.StemmingAnalyzer() | 
                whoosh.analysis.CharsetFilter(accent_map))

    schema = whoosh.fields.Schema(title         =whoosh.fields.TEXT(stored=True, analyzer=analyzer, field_boost=20.0), 
                                  artist_name   =whoosh.fields.TEXT(stored=True, analyzer=analyzer),
                                  artist_id     =whoosh.fields.NUMERIC(int, stored=True), 
                                  duration      =whoosh.fields.NUMERIC(int, stored=True),
                                  release_id    =whoosh.fields.STORED)

    index = whoosh.index.create_in(index_path, schema)

    return index.writer()

def load_ids(discogs_mapping):

    with open(discogs_mapping, 'r') as f:
        return pickle.load(f)['name_to_id']

def index_discogs(couch_url, n, index_dir, discogs_mapping):

    name_to_id = load_ids(discogs_mapping)

    server = couchdb.client.Server(url=couch_url)

    db = server['discogs_release']

    params = {'stale': 'ok', 'include_docs': True}

    if n > 0:
        params['limit'] = n

    writer = create_index_writer(index_dir)

    for i, doc in enumerate(db.view('_all_docs', **params)):

        if i % 1000 == 0:
            print i

        doc = doc['doc']

        artist_name = doc['artist']

        if artist_name not in name_to_id:
            continue


        artist_id = name_to_id[artist_name]
        release_id = doc['id']

        for t in doc['tracklist']:
            title  = t['title']
            duration    = duration_to_int(t['duration'])

            wdoc = {'artist_id': artist_id,
                    'release_id': release_id,
                    'title': unicode(title),
                    'artist_name': unicode(artist_name),
                    'duration': duration}

            writer.add_document(**wdoc)

    writer.commit()

def process_arguments(args):

    parser = argparse.ArgumentParser(description='Full-text index for discogs tracks')

    parser.add_argument('-n', '--num_releases',
                        dest='num_releases',
                        type=int,
                        required=False,
                        default=-1,
                        help='Maximum number of artists to process.')

    parser.add_argument('-u', '--url', 
                        dest='url', 
                        type=str, 
                        required=False,
                        default='http://localhost:5984', 
                        help='URL to the couchdb server')

    parser.add_argument(dest='discogs_mapping',
                        type=str,
                        help='Path to load discogs id mapping')

    parser.add_argument(dest='index_dir',
                        type=str,
                        help='Path to store whoosh index ')

    return vars(parser.parse_args(args))

if __name__ == '__main__':
    args =  process_arguments(sys.argv[1:])
    index_discogs(args['url'], args['num_releases'], args['index_dir'], args['discogs_mapping'])
