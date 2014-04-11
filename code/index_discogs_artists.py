#!/usr/bin/env python

import argparse
import os
import sys
import time

import cPickle as pickle

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

    schema = whoosh.fields.Schema(artist_name   =whoosh.fields.TEXT(stored=True, analyzer=analyzer),
                                  artist_id     =whoosh.fields.NUMERIC(int, stored=True))

    index = whoosh.index.create_in(index_path, schema)

    return index.writer()

def load_ids(discogs_mapping):

    with open(discogs_mapping, 'r') as f:
        return pickle.load(f)['name_to_id']

def index_discogs(index_dir, discogs_mapping):

    name_to_id = load_ids(discogs_mapping)

    writer = create_index_writer(index_dir)

    for artist_name, artist_id in name_to_id.iteritems():
            writer.add_document(artist_id=artist_id, artist_name=unicode(artist_name))

    writer.commit()

def process_arguments(args):

    parser = argparse.ArgumentParser(description='Full-text index for discogs tracks')

    parser.add_argument(dest='discogs_mapping',
                        type=str,
                        help='Path to load discogs id mapping')

    parser.add_argument(dest='index_dir',
                        type=str,
                        help='Path to store whoosh index ')

    return vars(parser.parse_args(args))

if __name__ == '__main__':
    args =  process_arguments(sys.argv[1:])
    index_discogs(args['index_dir'], args['discogs_mapping'])
