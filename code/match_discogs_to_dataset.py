#!/usr/bin/env python

import argparse
import sys
import ujson as json

import whoosh
import whoosh.index
import whoosh.qparser

from pprint import pprint

def process_arguments(args):

    parser = argparse.ArgumentParser(description='Full-text search for discogs tracks from a json file')


    parser.add_argument('-n', '--num_results',
                        dest='num_results',
                        type=int,
                        required=False,
                        default=1,
                        help='max # results to return')

    parser.add_argument('--tolerance', 
                        dest='tolerance', 
                        type=int, 
                        default=4,
                        help='duration tolerance (seconds)')

    parser.add_argument(dest='json_file',
                        type=str,
                        help='Path to input json file ')

    parser.add_argument(dest='index_dir',
                        type=str,
                        help='Path to store whoosh index ')

    parser.add_argument(dest='output_file', type=str, help='Path to output file')

    return vars(parser.parse_args(args))


def load_input(json_file):

    with open(json_file, 'r') as f:
        data = json.load(f)

    return data

def match_record(searcher, schema, artist, title, duration, tol=None, n=None):

    artist_parser = whoosh.qparser.SimpleParser('artist_name',  schema)
    title_parser  = whoosh.qparser.SimpleParser('title',        schema)
    dur_parser    = whoosh.qparser.SimpleParser('duration',     schema)
    dur_parser.add_plugin(whoosh.qparser.RangePlugin)
    
    q_artist = artist_parser.parse(artist)
    q_title  = title_parser.parse(title)

    # First try: artist, title, duration
    if tol:
        q_duration = dur_parser.parse('duration:{%d to %d}' % (duration - tol, duration + tol))

        q = whoosh.query.And([q_artist, q_title, q_duration])
        results = [dict(item) for item in searcher.search(q, limit=n)]
        if results:
            return results[0]

    # Second try: artist, title
    q = whoosh.query.And([q_artist, q_title])
    results = [dict(item) for item in searcher.search(q, limit=n)]
    if results:
        return results[0]

    # Third try: artist only
    q = q_artist
    results = [dict(item) for item in searcher.search(q, limit=n)]
    if results:
        return results[0]

    return None

def match_discogs(searcher, schema, data, output_file=None, tolerance=None, num_results=None):

    results = []
    for row in data:
        m = match_record(searcher, 
                        schema, 
                        row['artist'], 
                        row['title'],
                        row['duration'], 
                        tol=tolerance, n=num_results)

        if m:
            row['artist_id'] = m['artist_id']
        else:
            row['artist_id'] = None

        pprint(row)
        print
        results.append(row)

    with open(output_file, 'w') as f:
        json.dump(results, f)


if __name__ == '__main__':

    args = process_arguments(sys.argv[1:])

    data  = load_input(args.pop('json_file'))

    index = whoosh.index.open_dir(args.pop('index_dir'))
    with index.searcher() as search:
        match_discogs(search, index.schema, data, **args)

