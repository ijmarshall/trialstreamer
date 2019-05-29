'''
This is a utility module for mapping (extracted) PICO snippets to (respective) 
SciBERT embeddings. 

Requirements:
    - https://github.com/huggingface/pytorch-pretrained-BERT
    - https://github.com/allenai/scibert
'''

import torch
from pytorch_pretrained_bert import BertTokenizer, BertModel, BertForMaskedLM

import psycopg2

import json 

from trialstreamer import dbutil


def create_embedding_cols():
    add_col_str = '''alter table pubmed_pico
                    add column p_v float[],
                    add column i_v float[],
                    add column o_v float[];'''
    cur = dbutil.db.cursor()
    cur.execute(create_tables_command)
    cur.close()
    dbutil.db.commit()



class PICOBERT:

    def __init__(self, use_CUDA=False):
        ''' instantiate BERT '''

        # TODO this assumes an input size of 64, which is actually probably
        # about right for our purposes, but still might revisit.
        self.tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        self.model = BertModel.from_pretrained('bert-base-uncased')

        self.use_CUDA = use_CUDA
        if self.use_CUDA:
            self.model.to('cuda')

    def join_strs_for_BERT(self, list_of_snippets):
        ''' 
        Assemble a string for tokenization and consumption by BERT.
        We use [CLS] to denote start of sequences.
        '''
        num_snippets = len(list_of_snippets)
        if num_snippets == 0:
            return "", []
        else:
            combined_str = "[CLS] " + list_of_snippets[0]
            if num_snippets > 1:
                # this is more complex than it needs to be currently because was
                # originally thinking we wanted to preserve snippet order info.
                # we may want to revisit, as this is just naively concatenating
                # all snippets at the moment.
                for idx, snippet in enumerate(list_of_snippets[1:]):
                    combined_str += " " + snippet 
                    # segment_ids.extend([idx+1] * (len(snippet.split(" ")) + 1))
                
            combined_str += " [SEP]" # closing [SEP]
            
        # now tokenize
        tokenized_text = self.tokenizer.tokenize(combined_str)
        indexed_tokens = self.tokenizer.convert_tokens_to_ids(tokenized_text)


        # treating this as a single sequence / input. It is not obviously that this
        # is the right thing to do.
        segment_ids = [0]*len(tokenized_text)

        # move to tensors
        segment_ids =  torch.tensor([segment_ids]) 
        indexed_tokens = torch.tensor([indexed_tokens])
        if self.use_CUDA:
            segment_ids = segment_ids.to('cuda')
            indexed_tokens = indexed_tokens.to('cuda')

        return indexed_tokens, segment_ids


    @torch.no_grad()
    def extract_embedding(self, tokens, segments):
        #with torch.no_grad() as no_grad:
        encoded_layers, _ = self.model(tokens, segments)
         
        # TODO this 
        return encoded_layers[-1][0,0,:]

           
    def build_insert_str(self, id_, field, np_vec):
        ''' update pubmed_pico 
            set p_emb = '[0.43122005462646484, -0.22482353448867798, 0.30873173475265503, -0.011720098555088043, -0.02330666035413742, 0.06368391215801239, 0.3541651666164398, 0.29491424560546875]'
            where id=281328 '''
        
        # kind of terrible; probably better to store as json explicitly? 
        vec_str = json.dumps(np_vec.tolist()).replace("[", "{").replace("]", "}")
        q_str = '''update pubmed_pico set {0} = '{1}' where id={2}'''.format(field, vec_str, id_)
        return q_str


    def map_all_in_db(self):
        ''' 
        Populates the database with PICO embeddings, i.e., just embeds the
        extracted snippets via SciBERT.

        These are recoverable as numpy vectors -- just retrieve and run the 
        `p_v' record entries through np.array. e.g., assuming results stores
        the fetchall() yield:

            p_embedding = np.array(results[0]['p_v'])


        '''

        # First, create columns in the `pubmed_pico' table if not already there.

        # Next, retrieva all PICO snippets in the database
        cur = dbutil.db.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("select * from pubmed_pico;")    
        records = cur.fetchall()
        total = len(records)
        for i, r in enumerate(records):
            #print(r['id'])

            if i % 100 == 0:
                print ("on record {0} / {1}".format(i, total))
                
            if len(r['population']) > 0:
                p_tokens, p_segments = self.join_strs_for_BERT(r['population'])
                p_emb = self.extract_embedding(p_tokens, p_segments)
                p_str = self.build_insert_str(r['id'], 'p_v', p_emb)
                cur.execute(p_str)

            if len(r['interventions']) > 0:
                i_tokens, i_segments = self.join_strs_for_BERT(r['interventions'])
                i_emb = self.extract_embedding(i_tokens, i_segments)
                i_str = self.build_insert_str(r['id'], 'i_v', i_emb)
                cur.execute(i_str)

            
            if len(r['outcomes']) > 0:
                o_tokens, o_segments = self.join_strs_for_BERT(r['outcomes'])
                o_emb = self.extract_embedding(o_tokens, o_segments)
                o_str = self.build_insert_str(r['id'], 'o_v', o_emb)
                cur.execute(o_str)

