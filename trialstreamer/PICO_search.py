import sys

import numpy as np
import psycopg2

import tqdm 

# see https://github.com/spotify/annoy
from annoy import AnnoyIndex 

# assumes trialstreamer in your path
from trialstreamer import dbutil


BERT_EMBEDDING_SIZE = 768

def index_vecs(t, pmid, int_to_pmid, vecs, count):
    if vecs is None:
        return count

    for vec in vecs:
        int_to_pmid[count] = pmid 
        t.add_item(count, vec)
        count += 1
    return count

def build_annoy_indices(n_trees=10):
    ''' 
    Builds and returns an AnnoyIndex for `p_v', `i_v' or `o_v' -- Population,
    Intervention, Outcome, respectively.

    Assumes bert server is running. 
    '''
    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor, name='fetch_large_result')
    t_p, t_i, t_o  = AnnoyIndex(BERT_EMBEDDING_SIZE), AnnoyIndex(BERT_EMBEDDING_SIZE), AnnoyIndex(BERT_EMBEDDING_SIZE) 
    cur.execute('select pmid, p_v, i_v, o_v from pubmed_pico;')
   
    int_to_pmid_p, int_to_pmid_i, int_to_pmid_o = {}, {}, {}
    count_p, count_i, count_o = 0, 0, 0
    i = 0
    batch_size = 1000
    while True:
        if i % 10 == 0:
            print("on iter {0}".format(i))

        # consume result over a series of iterations
        # with each iteration fetching a batch of records
        records = cur.fetchmany(size=batch_size)

        if not records:
            break
        
        for r in tqdm.tqdm(records, desc = "iter {} ({} done)".format(i, i*batch_size)):
            pmid = r['pmid']
            
            p_vecs = r['p_v']
            count_p = index_vecs(t_p, pmid, int_to_pmid_p, p_vecs, count_p)

            i_vecs = r['i_v']
            count_i = index_vecs(t_i, pmid, int_to_pmid_i, i_vecs, count_i)

            o_vecs = r['o_v']
            count_o = index_vecs(t_o, pmid, int_to_pmid_o, o_vecs, count_o)

        i += 1
   
    print("finished! building and dumping.")

    t_p.build(n_trees)
    t_p.save('p.ann')
    with open("int_to_pmid_p.pkl","wb") as outf:
        pickle.dump(int_to_pmid_p, outf)

    t_i.build(n_trees)
    t_i.save('i.ann')
    with open("int_to_pmid_i.pkl","wb") as outf:
        pickle.dump(int_to_pmid_i, outf)

    t_o.build(n_trees) 
    t_o.save('o.ann')
    with open("int_to_pmid_o.pkl","wb") as outf:
        pickle.dump(int_to_pmid_o, outf)

    print("indices saved!")

  
if __name__ == '__main__':
    build_annoy_indices()
