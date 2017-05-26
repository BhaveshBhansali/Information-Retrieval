import json

from elasticsearch import Elasticsearch
from pprint import pprint

class indexing:

    es = Elasticsearch()

    def read_document_to_index(self,file_to_read):

        json_document_data=[]
        with open('/.'+str(file_to_read)) as f:
            for line in f:
                json_document_data.append(json.loads(line))
        return json_document_data


    def create_index_BM25(self,index_name):

        mapping = '''
        {
           "mappings": {
              "json_document_data_bm25": {
                 "properties": {
                    "id": {
                       "type": "string",
                       "similarity": "BM25"
                    },
                    "content": {
                       "type": "string",
                       "similarity": "BM25"
                    },
                    "title": {
                       "type": "string",
                       "similarity": "BM25"
                    },
                    "pub-date": {
                       "type": "string",
                       "similarity": "BM25"
                    }
                 }
              }
           }
        }
        '''

        self.es.indices.create(index=str('atir_docs_bm25'), ignore=400, body=mapping)


    def creat_index_language_analyzer(self,index_name):
        mapping = '''
        {
            "settings": {
                    "analysis": {
                        "filter": {
                            "stop_filter": {
                                    "type": "stop",
                                    "stopwords": ["_english_"]
                            },
                            "custom_english_stemmer": {
                                "type": "stemmer",
                                "name": "minimal_english"
                                }
                        },
                        "analyzer": {
                            "custom_lowercase_stemmed": {
                                "tokenizer": "standard",
                                "filter": [
                                    "stop_filter",
                                    "custom_english_stemmer",
                                    "lowercase"
                                ]
                            }
                        }
                    }
            }
        }
        '''

        self.es.indices.create(index=str(index_name), ignore=400, body=mapping)



    def load_data_on_index(self,data,index_name,document_type):

        for item in data:
            id = item.get('id')
            self.es.index(index=str(index_name),doc_type=str(document_type),id=id, body=item)




def main():

    json_document_data=indexing.read_document_to_index('documents.json')

    # Create BM25 indexing model
    indexing.create_index_BM25('atir_docs_bm25')
    indexing.load_data_on_index(json_document_data,'atir_docs_bm25','json_document_data_25')

    # Create language Analyzer indexing Model
    indexing.create_index_BM25('atir_docs_analyzer')
    indexing.load_data_on_index(json_document_data,'atir_docs_analyzer','json_document_data_25')



if __name__ == '__main__':
    main()