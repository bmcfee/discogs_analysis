#!/usr/bin/env python

import argparse
import sys
from pprint import pprint

import whoosh
import whoosh.index
import whoosh.qparser

def process_arguments(args):

    parser = argparse.ArgumentParser(description='Full-text search for discogs tracks')


    parser.add_argument('-a', '--artist', 
                        dest='artist', 
                        type=unicode, 
                        required=False,
                        help='artist string')

    parser.add_argument('-t', '--title', 
                        dest='title', 
                        type=unicode, 
                        required=False,
                        help='title string')

    parser.add_argument('-d', '--duration', 
                        dest='duration', 
                        type=int, 
                        required=False,
                        default=None,
                        help='duration (seconds)')

    parser.add_argument('-n', '--num_results',
                        dest='num_results',
                        type=int,
                        required=False,
                        default=5,
                        help='max # results to return')

    parser.add_argument('--tolerance', 
                        dest='tolerance', 
                        type=int, 
                        default=4,
                        help='duration tolerance (seconds)')

    parser.add_argument(dest='index_dir',
                        type=str,
                        help='Path to store whoosh index ')

    return vars(parser.parse_args(args))

def probe(searcher, schema, artist=None, title=None, duration=None, tolerance=None,
num_results=None):


    dur_parser      = whoosh.qparser.SimpleParser('duration', schema)
    dur_parser.add_plugin(whoosh.qparser.RangePlugin)
    if duration and tolerance:
        q_duration      = dur_parser.parse('duration:{%d to %d}' % (duration-tolerance, duration+tolerance))
    else:
        q_duration      = None

    title_parser    = whoosh.qparser.SimpleParser('title', schema)
    q_title = title_parser.parse(title)
    # First: try for a title and artist match
    if artist:
        artist_parser   = whoosh.qparser.SimpleParser('artist_name', schema)
        q_artist = artist_parser.parse(artist)

        q = whoosh.query.And([q_artist, q_title])
        if q_duration:
            q = whoosh.query.And([q, q_duration])

        results = [dict(item) for item in searcher.search(q, limit=num_results)]
        if results:
            return results

    q = q_title
    if q_duration:
        q = whoosh.query.And([q, q_duration])

    results = [dict(item) for item in searcher.search(q, limit=num_results)]

    if results or not q_duration:
        return results

    q = q_title
    results = [dict(item) for item in searcher.search(q, limit=num_results)]

    return results
        



if __name__ == '__main__':

    args = process_arguments(sys.argv[1:])

    index = whoosh.index.open_dir(args.pop('index_dir'))

    with index.searcher() as search:
        pprint( probe(search, index.schema, **args) )
