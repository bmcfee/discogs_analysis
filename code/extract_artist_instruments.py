#!/usr/bin/env python

import argparse
import sys
import shelve
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
    
    if sd is None:
        sd = nltk.data.load('tokenizers/punkt/english.pickle')
    
    # NLTK doesn't tokenize on / 
    query = query.replace('/', ' ')    
    results = []
    for l in sd.tokenize(query):
        results.extend(analyze_sentence(l))
    
    tags = set()
    for w, p in results:
        if is_instrumental(w):#, pos=get_wordnet_pos(p)):
            tags.add(w.lower())
            
    return tags

# Only yield documents with a non-empty profile
def profile_filter(doc):
    if doc['profile'].strip():
        yield None, doc


def extract_keywords(url, max_artists, data):
    server = couchdb.client.Server(url=url)
    db = server['discogs_artist']

    data = shelve.open(data)

    for i, doc in enumerate(db.view('has_profile/has_profile')):
        if max_artists > 0 and i > max_artists:
            break
        
        data['id'] = {'name': doc.value['name'], 'terms': analyze_text(doc.value['profile'])}
        print doc.value['id'], doc.value['_id']
        print doc.value['name']
        print '\t', analyze_text(doc.value['profile'])
        print
        print doc.value['profile']
        print '---\n'


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

    extract_keywords(args['url'], args['num_artists'], args['data'])

