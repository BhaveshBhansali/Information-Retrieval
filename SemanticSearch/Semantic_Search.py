from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
from elasticsearch import Elasticsearch
import json


class semantic_search:


    #function to extract named entities of the text using Stanford Tagger
    def stanford_tagger(text) :
        named_entity=[]
        st = StanfordNERTagger('english.all.3class.distsim.crf.ser.gz','stanford-ner/stanford-ner.jar',encoding='utf-8')
        tokenized_text = word_tokenize(text)
        classified_text = st.tag(tokenized_text)
        for tuples in classified_text:
            if tuples[1]== 'PERSON' or tuples[1]== 'LOCATION' or tuples[1]== 'ORGANIZATION':
                named_entity.append(tuples)
        return named_entity

    def update_doc_with_named_entity(self,document_name,index_name):

        es = Elasticsearch()
        json_document_data=[]
        with open("./+"+document_name) as f:
            for line in f:
                json_document_data.append(json.loads(line))


        for item in json_document_data:
            id = item.get('id')
            named_entity=self.stanford_tagger(item.get('content'))
            es.update(index=index_name,doc_type='json_document_data',id=id, body={"doc":{"named_entities":named_entity}})



def main():

    semantic_search.update_doc_with_named_entity('document.json','atir_docs')

if __name__ == '__main__':
    main()