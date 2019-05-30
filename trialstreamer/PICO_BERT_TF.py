'''
Uses SciBERT (TensorFlow version) via bert-as-service: https://github.com/hanxiao/bert-as-service/.

Follow the instructions here: https://github.com/hanxiao/bert-as-service/#getting-started. 

Basically, this entails pip installing the server and client, and the SciBERT weights available
here: https://github.com/allenai/scibert (BERT-Base, Uncased: ).

This can be run on the CPU using an explicit -cpu flag. Note also the max_seq_len flag (which will otherwise 
default to 25).

    > bert-serving-start -model_dir=path/to/SciBERT-weights -cpu -max_seq_len=64

Note that it is sort of absurd to have this module given how little code is here, but this way we 
can modify BERT encoder easily.
'''

from bert_serving.client import BertClient

class PICOBERT_TF:

    def __init__(self):
        ''' instantiate BERT client. '''
        self.bert = BertClient()

           
    def encode(self, snippets):
        return self.bert.encode(snippets)

