#!/usr/bin/env python

import argparse
import cPickle as pickle
import sys
import couchdb

import nltk
import nltk.tokenize.punkt

from nltk.corpus import wordnet


def is_instrumental(w, pos=None, key_tags=None):
    
    if key_tags is None:
        key_tags = set(['musician.n.01', 'musical_instrument.n.01', 'percussion.n.01', 'vocal_music.n.01'])
        
    for s in wordnet.synsets(w, pos=pos):
        for path in s.hypernym_paths():
            hypernyms = set([p.name for p in path])
            
            if hypernyms & key_tags:
                return True
            
    return False


def get_wordnet_pos(treebank_tag):

    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return None


def analyze_text(query, sd=None):
    
    def analyze_sentence(q):
        tokens = nltk.tokenize.word_tokenize(q)
        return nltk.pos_tag(tokens)
    
    tags = set()
    if not len(query):
        return tags

    # NLTK doesn't tokenize on / 
    query = query.replace('/', ' ')    
    results = []
    for l in sd.tokenize(query):
        results.extend(analyze_sentence(l))
    
    for w, p in results:
        if is_instrumental(w):#, pos=get_wordnet_pos(p)):
            tags.add(w.lower())
            
    return tags

def analyze_discogs(url, max_artists, data_file):
    server = couchdb.client.Server(url=url)

    db = server['discogs_artist']

    id_to_terms     = {}
    id_to_name      = {}
    name_to_id      = {}
    groups          = {}
    members         = {}


    sd = nltk.data.load('tokenizers/punkt/english.pickle')

    if max_artists < 0:
        max_artists = None

    for doc in db.view('_all_docs', limit=max_artists, stale='ok', include_docs=True):
        doc = doc['doc']

        if len(id_to_name) % 1000 == 0:
            print len(id_to_name)

        # Connect id<=>name
        id_to_name[doc['id']]   = doc['name']
        name_to_id[doc['name']] = doc['id']
        
        # Analyze profile text for instrument terms
        terms = analyze_text(doc['profile'], sd=sd)

        id_to_terms[doc['id']]  = terms

        # Analyze the membership graph
        groups[doc['id']]       = doc['groups']
        members[doc['id']]      = doc['members']


    # Convert memberships from name-oriented to id-oriented
    artist_to_group = {}
    group_to_artist = {}

    for artist_id, artist_groups in groups.iteritems():
        if artist_id not in artist_to_group:
            artist_to_group[artist_id] = set()

        for x in artist_groups:
            if x in name_to_id:
                # If the group is in the id space, add it to the artist
                artist_to_group[artist_id].add(name_to_id[x])

                # And add the artist to the group
                if x not in group_to_artist:
                    group_to_artist[x] = set()
                group_to_artist[x].add(artist_id)

    for group_id, group_members in members.iteritems():
        if group_id not in group_to_artist:
            group_to_artist[group_id] = set()

        for x in group_members:
            if x in name_to_id:
                group_to_artist[group_id].add(name_to_id[x])

                if x not in artist_to_group:
                    artist_to_group[x] = set()

                artist_to_group[x].add(group_id)

    with open(data_file, 'w') as f:
        pickle.dump({'id_to_terms': id_to_terms, 
                     'id_to_name': id_to_name, 
                     'name_to_id': name_to_id,
                     'artist_to_group': artist_to_group,
                     'group_to_artist': group_to_artist}, 
                     f, protocol=-1)

def process_arguments(args):

    parser = argparse.ArgumentParser(description='Instrument keyword extractor for discogs artists')

    parser.add_argument('-n', '--num_artists',
                        dest='num_artists',
                        type=int,
                        required=False,
                        default=50,
                        help='Maximum number of artists to process.  Use -1 to disable')

    parser.add_argument('-u', '--url', 
                        dest='url', 
                        type=str, 
                        required=False,
                        default='http://localhost:5984', 
                        help='URL to the couchdb server')

    parser.add_argument('-d', '--data',
                        dest='data',
                        type=str,
                        required=False,
                        default='artist_keywords.pickle',
                        help='Path to store artist keyword index (pickle)')

    return vars(parser.parse_args(args))

if __name__ == '__main__':
    args = process_arguments(sys.argv[1:])

    analyze_discogs(args['url'], args['num_artists'], args['data'])

