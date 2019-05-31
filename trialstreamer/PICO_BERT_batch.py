'''
Code to generate PICO embeddings for articles in the RCT database in batch.
'''

import psycopg2
import json 

from trialstreamer import dbutil, PICO_BERT_TF
import trialstreamer

import logging
import tqdm
from itertools import zip_longest

log = logging.getLogger(__name__)

'''
A bit of code to add embedding columns to the pubmed_pico table
IM: assuming we won't run this in practice now have amended database (have added this to init code for new DBs)
'''
# def create_embedding_cols():
#     create_tables_command = '''alter table pubmed_pico
#                     add column p_v float[],
#                     add column i_v float[],
#                     add column o_v float[];'''
#     cur = dbutil.db.cursor()
#     cur.execute(create_tables_command)
#     cur.close()
#     dbutil.db.commit()


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks - from itertools recipes adapted a bit"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return ([i for i in r if i is not None] for r in zip_longest(*args, fillvalue=fillvalue))



def build_insert_str(id_, field, np_vec):
    ''' update pubmed_pico 
        set p_emb = '[0.43122005462646484, -0.22482353448867798, 0.30873173475265503, -0.011720098555088043, -0.02330666035413742, 0.06368391215801239, 0.3541651666164398, 0.29491424560546875]'
        where id=281328 '''
    
    # kind of terrible; probably better to store as json explicitly? 
    vec_str = json.dumps(np_vec.tolist()).replace("[", "{").replace("]", "}")
    q_str = '''update pubmed_pico set {0} = '{1}' where id={2}'''.format(field, vec_str, id_)
    return q_str



def map_all_in_db(force_refresh=False):
    ''' 
    Populates the database with PICO embeddings, i.e., just embeds the
    extracted snippets via SciBERT.

    These are recoverable as numpy vectors -- just retrieve and run the 
    `p_v' record entries through np.array. e.g., assuming results stores
    the fetchall() yield:

        p_embedding = np.array(results[0]['p_v'])

    This assumes that the pubmed_pico table has been modified to include
    vector columns.
    '''

    log.info('instantiating BERT')
    bert = PICO_BERT_TF.PICOBERT_TF()

    log.info('retrieve all PICO snippets in the database')
    read_cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    update_cur = dbutil.db.cursor()

    if force_refresh:
        # deleting doi table from database
        log.info('redoing all PICO BERTs...')
        log.info('getting source data')
        read_cur.execute("SELECT id, population, interventions, outcomes FROM pubmed_pico;")
        records = read_cur.fetchall()
    else:
        log.info('calculating PICO BERTs for new records only...')
        log.info('getting source data')
        read_cur.execute("SELECT id, population, interventions, outcomes FROM pubmed_pico where (p_v is null) or (i_v is null) or (o_v is null);")
        records = read_cur.fetchall()

    log.info('calculating number of records to process')

    
    batch_size = 100

    log.info('processing the BERTs!')

    for batch in tqdm.tqdm(grouper(records, batch_size),  desc="articles processed for BERT embeddings"):
        
        for r in batch:           
            def filter_empty(snippets):
                return [s for s in snippets if s!=""]
        
            p_snippets = filter_empty(r['population'])
            if len(p_snippets) > 0:
                p_emb = bert.encode(p_snippets)
                p_str = build_insert_str(r['id'], 'p_v', p_emb)
                update_cur.execute(p_str)

            i_snippets = filter_empty(r['interventions'])
            if len(i_snippets) > 0:
                i_emb = bert.encode(i_snippets)
                i_str = build_insert_str(r['id'], 'i_v', i_emb)
                update_cur.execute(i_str)

            o_snippets = filter_empty(r['outcomes'])
            if len(o_snippets) > 0:
                o_emb = bert.encode(o_snippets)
                o_str = build_insert_str(r['id'], 'o_v', o_emb)
                update_cur.execute(o_str)

        dbutil.db.commit()

if __name__ == '__main__':
    map_all_in_db()

