import elasticsearch
import json
import collections
import math
from decimal import *
from SearchResults.result_clustering_and_labelling import result_clustering

class mmr:

    es = elasticsearch.Elasticsearch("127.0.0.1:9200")


    #function to calculate cosine similarity between two vectors
    def get_cosine(vec1, vec2):
       intersection = set(vec1.keys()) & set(vec2.keys())
       numerator = sum([vec1[x] * vec2[x] for x in intersection])

       sum1 = sum([vec1[x]**2 for x in vec1.keys()])
       sum2 = sum([vec2[x]**2 for x in vec2.keys()])
       denominator = math.sqrt(sum1) * math.sqrt(sum2)

       if not denominator:
           return 0.0
       else:
           return float(numerator) / denominator


    def mmr_results(self,query_string,lamda_value):
        term_vectors_all_id={}
        term_vectors_dict={}
        doc_id_q_score={}
        doc_list=[]
        final_mmr_result=[]

        resdocs = result_clustering.search(self.es,query_string, 100)
        document_length=48593



        #Computing Similarity Score for all retrieved results
        for docs in resdocs['hits']['hits']:
            doc_id_q_score[docs['_id']]=docs['_score']
            doc_list.append(docs['_id'])
            for key, value in docs.items():
              if key == '_source':
                  term_vectors_all_id = self.es.termvector(index='atir16trecindex',doc_type='doc', id=value['id'],params=None)
                  All_terms=term_vectors_all_id['term_vectors']['content']['terms']
                  term_vectors_dict[value['id']]={}
                  for i in All_terms:
                        term=i
                        term_values=All_terms[i]
                        doc_freq=All_terms[i]['doc_freq']
                        term_freq=All_terms[i]['term_freq']
                        tf_idf=term_freq*math.log(document_length/doc_freq)
                        term_vectors_dict[value['id']][i]=tf_idf

        #Computing Maximum Marginal Relevance

        #Inserting first document which has highest sim(q,d)
        final_mmr_result.append(doc_list[0])
        del doc_id_q_score[doc_list[0]]


        #Adding more documents using greedy selection rule and computing sim(d,d')
        for i in range(1,10):
            max_next_document_score= float('-inf')
            next_document_mmr=0
            for key_doc_id,value_score in doc_id_q_score.items():
                prev_max_similarity_already_ranked_doc_score=0
                for doc in final_mmr_result:
                    similarity_already_ranked_doc_score=self.get_cosine(term_vectors_dict[doc],term_vectors_dict[key_doc_id])
                    if similarity_already_ranked_doc_score > prev_max_similarity_already_ranked_doc_score:
                        prev_max_similarity_already_ranked_doc_score = similarity_already_ranked_doc_score

                next_document_score=lamda_value*value_score-(1-lamda_value)*prev_max_similarity_already_ranked_doc_score
                #print(next_document_score)
                if next_document_score > max_next_document_score:
                    max_next_document_score = next_document_score
                    next_document_mmr=key_doc_id


            final_mmr_result.append(next_document_mmr)
            del doc_id_q_score[next_document_mmr]

        return final_mmr_result


def main():

    query_string="Alzheimer's Drug Treatment"
    lambda_value=0.1

    mmr_results=mmr.mmr_results(query_string,lambda_value)
    print(mmr_results)

if __name__ == '__main__':
    main()