#!/usr/bin/env python

import argparse
import sys

import ujson as json

import gordon

def get_track_info(collection_name=None, output_file=None):

    collection = gordon.Collection.query.filter_by(name=collection_name).limit(1).all()

    if len(collection) == 0:
        raise ValueError('Empty collection: %s' % collection_name)

    track_data = []
    for t in collection[0].tracks:
        track_data.append({'title': unicode(t.title), 'artist': unicode(t.artist), 'duration': int(t.secs)})

    with open(output_file, 'w') as f:
        json.dump(track_data, f)

    pass


def process_arguments(args):

    parser = argparse.ArgumentParser(description='Get a collection out of gordon')

    parser.add_argument('collection_name', type=str, action='store', help='Which collection to extract')
    parser.add_argument('output_file', type=str, action='store', help='Path to store json output')

    return vars(parser.parse_args(args))

if __name__ == '__main__':
    args = process_arguments(sys.argv[1:])

    get_track_info(**args)
