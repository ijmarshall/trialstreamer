'''
Code to generate PICO embeddings for articles in the RCT database in batch.
'''

import psycopg2
import json 

from trialstreamer import dbutil, PICO_BERT_TF


'''
A bit of code to add embedding columns to the pubmed_pico table
'''
def create_embedding_cols():
    add_col_str = '''alter table pubmed_pico
                    add column p_v float[],
                    add column i_v float[],
                    add column o_v float[];'''
    cur = dbutil.db.cursor()
    cur.execute(create_tables_command)
    cur.close()
    dbutil.db.commit()


def build_insert_str(id_, field, np_vec):
    ''' update pubmed_pico 
        set p_emb = '[0.43122005462646484, -0.22482353448867798, 0.30873173475265503, -0.011720098555088043, -0.02330666035413742, 0.06368391215801239, 0.3541651666164398, 0.29491424560546875]'
        where id=281328 '''
    
    # kind of terrible; probably better to store as json explicitly? 
    vec_str = json.dumps(np_vec.tolist()).replace("[", "{").replace("]", "}")
    q_str = '''update pubmed_pico set {0} = '{1}' where id={2}'''.format(field, vec_str, id_)
    return q_str



def map_all_in_db():
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

    # instantiate BERT
    bert = PICO_BERT_TF.PICOBERT_TF()


    # retrieve all PICO snippets in the database
    cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("select * from pubmed_pico;")    
    records = cur.fetchall()
    total = len(records)
    for i, r in enumerate(records):

        if i % 100 == 0:
            print ("on record {0} / {1}".format(i, total))
            
        #import pdb; pdb.set_trace()
        if len(r['population']) > 0:
            p_emb = bert.encode(r['population'])
            p_str = build_insert_str(r['id'], 'p_v', p_emb)
            cur.execute(p_str)

        if len(r['interventions']) > 0:
            i_emb = bert.encode(r['interventions'])
            i_str = build_insert_str(r['id'], 'i_v', i_emb)
            cur.execute(i_str)

        
        if len(r['outcomes']) > 0:
            o_emb = bert.encode(r['outcomes'])
            o_str = build_insert_str(r['id'], 'o_v', o_emb)
            cur.execute(o_str)

