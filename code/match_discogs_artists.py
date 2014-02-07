#!/usr/bin/env python
'''CREATED:2014-02-07 14:24:16 by Brian McFee <brm2132@columbia.edu>

Match discogs artist names to the Million Song Dataset.

Relies upon the fulltext index builder: https://github.com/bmcfee/msd_search
'''

import argparse
import sys
import couchdb
import whoosh
import whoosh.index
import whoosh.qparser

from pprint import pprint


def search_artists(index, name, num_results=None):

    if isinstance(name, str):
        name = unicode(name, errors='ignore')

    q = whoosh.qparser.QueryParser('artist_name', index.schema).parse(name)

    with index.searcher() as search:
        return [(item.score, dict(item)) for item in search.search(q, limit=num_results)]

    return None

def match_artists(index, server, num_results):

    db = server['discogs_artist']

    for i, doc in enumerate(db):
        if i > 50:
            break
        
        mydoc = dict(db[doc])

        pprint(mydoc)

        names = [mydoc['name']]
        names.extend(mydoc['namevariations'])
        names.extend(mydoc['aliases'])

        hits = {}
        hits[mydoc['name']] = search_artists(index, mydoc['name'], num_results=num_results)

#         for nv in mydoc['namevariations']:
#             hits[nv] = search_artists(index, nv, num_results=num_results)

#         for nv in mydoc['aliases']:
#             hits[nv] = search_artists(index, nv, num_results=num_results)

        pprint(hits)

        print

def process_arguments(args):

    parser = argparse.ArgumentParser(description='Map discogs artist data to MSD artist data')

    parser.add_argument('-c', '--couch-db', 
                        dest='couch_url', 
                        type=str, 
                        required=True, 
                        help='URL to couchdb server')

    parser.add_argument('index_dir', 
                        action='store', 
                        help='Path to MSD artist index')

    parser.add_argument('-n', '--num-results', 
                        dest='num_results', 
                        type=int, 
                        default=10, 
                        help='Maximum number of results to match')

    return vars(parser.parse_args(args))


if __name__ == '__main__':

    args = process_arguments(sys.argv[1:])

    # Load the index
    index = whoosh.index.open_dir(args['index_dir'])

    # Build the couchdb connection
    server  = couchdb.client.Server(url=args['couch_url'])

    # Do the work
    match_artists(index, server, num_results=args['num_results'])

