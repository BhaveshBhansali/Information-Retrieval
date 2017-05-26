from elasticsearch import Elasticsearch
import os


class query_processing:

    #creating object of the elasticsearch
    es = Elasticsearch()

    def split_file_on_tab(self,file_name):

        with open('queries.tsv', 'r') as tsv:
            splited_file = [line.strip().split('\t') for line in tsv]
        return splited_file


    def search_result(self,query_array,judgements,index_name):


        res = self.es.search(index=index_name, size=10000,analyzer=True, body={"query":
                                             {"query_string":
                                              {"default_field": "title", "query": str(query_array).replace("/"," ")
                                               }
                                              }
                                         }
                            )

        relevant_dict={}
        irrelevant_dict={}
        count_relevant=0
        count_irrelevant=0
        relevant_list=[]
        irrelevant_list=[]
        query_search_result=[]

        #Creating list of relevant and irrelevant documents for each query
        for judgements_arr in judgements:
            if (judgements_arr[0]==query_array[0]):
                if (judgements_arr[2]=="1"):
                    count_relevant=count_relevant+1
                    relevant_dict[query_array[0]]=count_irrelevant
                    relevant_list.append(judgements_arr[1])
                elif (judgements_arr[2]=="0"):
                    count_irrelevant=count_irrelevant+1
                    irrelevant_dict[query_array[0]]=count_irrelevant
                    irrelevant_list.append(judgements_arr[1])


        #Creating document list from search results
        for hit in res['hits']['hits']:
            query_search_result.append(hit["_source"]["id"])

        return relevant_dict, irrelevant_dict, count_relevant, count_irrelevant, relevant_list, irrelevant_list,query_search_result

    def count_relevant_search(self,query_search_result,relevant_list):

        count_rel_search=0
        for i in query_search_result:
            for j in relevant_list:
                if i==j:
                    count_rel_search = count_rel_search+1
        return count_rel_search



    def precsion_calulation(self,query_search_result,count_rel_search):

        if len(query_search_result)!=0:
            precision=count_rel_search/len(query_search_result)
        return precision


    def recall_calculation(self,relevant_list,count_rel_search):

        if len(relevant_list)!=0:
            recall=count_rel_search/len(relevant_list)


def main():

    query=query_processing.split_file_on_tab('queries.tsv')
    judgements=query_processing.split_file_on_tab('jusdgements.tsv')

    #Processing each query for the search results and calculating precision,recall
    for query_array in query:
        relevant_dict, irrelevant_dict, count_relevant, count_irrelevant, relevant_list, irrelevant_list,query_search_result=query_processing.search_result(str(query_array[1]),judgements)

        #Calculating nummber of relevant search results
        count_rel_search=query_processing.count_relevant_search(query_search_result,relevant_list,'atir_docs_bm25')

        # Calculating Precision for Each Query
        query_processing.precsion_calulation(query_search_result,count_rel_search)

        #Calculating recall for each query
        query_processing.recall_calculation(relevant_list,count_rel_search)






if __name__ == '__main__':
    main()