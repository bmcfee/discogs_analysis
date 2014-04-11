#!/usr/bin/env python

import argparse
import sys
import ujson as json

import whoosh
import whoosh.index
import whoosh.qparser
import re

from Levenshtein import distance as edit_distance

from pprint import pprint


def process_arguments(args):

    parser = argparse.ArgumentParser(description='Full-text search for discogs tracks from a json file')


    parser.add_argument('-n', '--num_results',
                        dest='num_results',
                        type=int,
                        required=False,
                        default=10,
                        help='max # results to return')

    parser.add_argument(dest='json_file',
                        type=str,
                        help='Path to input json file ')

    parser.add_argument(dest='index_dir',
                        type=str,
                        help='Path to store whoosh index ')

    parser.add_argument(dest='output_file', type=str, help='Path to output file')

    return vars(parser.parse_args(args))

def scrub_string(x):
    # Kill parentheticals
    x = re.sub('\(.*?\)', '', x)

    return x.strip()

def load_input(json_file):

    with open(json_file, 'r') as f:
        data = json.load(f)

    return data

def make_results_list(res, artist_name):

    results = []

    for item in map(dict, res):
        if edit_distance(artist_name, item['artist_name']) > 1 + max(len(artist_name), len(item['artist_name']) ) / 2:
            continue
        results.append(item)

    return results

def match_record(searcher, schema, artist, n=None):

    artist_parser = whoosh.qparser.SimpleParser('artist_name',  schema)
    
    q_artist = artist_parser.parse(artist)

    # Third try: artist only
    q = q_artist
    results = make_results_list(searcher.search(q, limit=n), artist)
    return results


def match_discogs(searcher, schema, data, output_file=None, num_results=None):

    results = []
    for row in data:
        m = match_record(searcher, 
                        schema, 
                        scrub_string(row['artist']), 
                        n=num_results)

        row['matches'] = m

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

