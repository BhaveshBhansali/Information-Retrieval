import json
import sys
from collections import Set
from random import randint
import random
import elasticsearch
import re
from nltk.corpus import stopwords
from nltk import ne_chunk, pos_tag, word_tokenize
from nltk.tree import Tree



class result_clustering:

    es = elasticsearch.Elasticsearch("127.0.0.1:9200")

    def search(es, string_query, k):
        """Simple Elasticsearch Query"""
        query = json.dumps(
         {
         "query":{
            "query_string" : {
                   "default_field" : "content",
                    "query": string_query
            }
            }, "size": k
          }

        )
        results = es.search(index = 'atir16trecindex', body = query)

        return results



    def get_continuous_chunks(text):
        chunked = ne_chunk(pos_tag(word_tokenize(text)))
        prev = None
        continuous_chunk = []
        current_chunk = []
        for i in chunked:
             if type(i) == Tree:
                    current_chunk.append(" ".join([token for token, pos in i.leaves()]))
             elif current_chunk:
                    named_entity = " ".join(current_chunk)
                    if named_entity not in continuous_chunk:
                        continuous_chunk.append(named_entity)
                        current_chunk = []
             else:
                    continue
        return continuous_chunk


    def recompute_centroid(self,centroid,cluster):

        minnimum_dist=1
        distance=0
        min_val_key=None
        for key1 in cluster:
            for key2 in cluster:
                distance=distance+1-len(cluster[key1].intersection(cluster[key2]))/len(cluster[key1].union(cluster[key2]))
            avg_distance=distance/len(cluster)
            if avg_distance <= minnimum_dist:
                minnimum_dist = avg_distance
                min_val_key = key1
        centroid.append(cluster[min_val_key])


    def results_clustering(self,resdocs):


        list_of_res_id = []
        centroid = []
        list_of_res_content = []
        dictionary_result = {}

        for docs in resdocs['hits']['hits']:
            for key, value in docs.items():
              if key == '_source':
                temp_str=None
                temp_str=value['content']
                str_words=temp_str.split(" ")
                Set_words=set(str_words)
                filtered_words=[word for word in Set_words if word not in stopwords.words('english')]
                Set_words=set(filtered_words)

                list_of_res_id.append(value['id'])
                list_of_res_content.append(Set_words)
                dictionary_result.update({value['id']:Set_words})

        #Initilizing 10 centroids at the beginning
        for x in range (0, 10):

            rand_num=random.randint(0,len(list_of_res_content))
            centroid.append(list_of_res_content.__getitem__(rand_num))

        #100 Iterations to conevrge and to get 10 clusters of documents
        for iteration in range(0,100):
            cluster_one={}
            cluster_two={}
            cluster_three={}
            cluster_four={}
            cluster_five={}
            cluster_six={}
            cluster_seven={}
            cluster_eight={}
            cluster_nine={}
            cluster_ten={}

            #Finding and assigning cluster for each result document
            for key in dictionary_result:
                min_dist=1
                c_index=0


                #Finding which centroid is closest to document
                for c in centroid:
                    dist=1-len(c.intersection(dictionary_result[key]))/len(c.union(dictionary_result[key]))
                    if dist <= min_dist:
                        min_dist=dist
                        c_index=centroid.index(c)

                #Assigning documents to it's nearest centroid
                if c_index == 0:
                    cluster_one.update({key:dictionary_result[key]})
                elif c_index == 1:
                    cluster_two.update({key:dictionary_result[key]})
                elif c_index == 2:
                    cluster_three.update({key:dictionary_result[key]})
                elif c_index == 3:
                    cluster_four.update({key:dictionary_result[key]})
                elif c_index == 4:
                    cluster_five.update({key:dictionary_result[key]})
                elif c_index == 5:
                    cluster_six.update({key:dictionary_result[key]})
                elif c_index == 6:
                    cluster_seven.update({key:dictionary_result[key]})
                elif c_index == 7:
                    cluster_eight.update({key:dictionary_result[key]})
                elif c_index == 8:
                    cluster_nine.update({key:dictionary_result[key]})
                elif c_index == 9:
                    cluster_ten.update({key:dictionary_result[key]})

            #Recomputing new centroid of each cluster based on intersection of words among current members of cluster
            centroid=[]
            self.recompute_centroid(centroid,cluster_one)
            self.recompute_centroid(centroid,cluster_two)
            self.recompute_centroid(centroid,cluster_three)
            self.recompute_centroid(centroid,cluster_five)
            self.recompute_centroid(centroid,cluster_six)
            self.recompute_centroid(centroid,cluster_seven)
            self.recompute_centroid(centroid,cluster_eight)
            self.recompute_centroid(centroid,cluster_nine)
            self.recompute_centroid(centroid,cluster_ten)

        return centroid

    def label_for_cluster(self,centroid,query_name):
        output_file=open('./'+str(query_name)+'.txt',mode='w')
        output_file.write("\n-----------------------------------------------------")
        output_file.write("\n Labels for each Cluster")
        cluster_no=0
        for i in centroid:
            entity_recognier_list=[]
            output_file.write("\n--------------------------------------")
            output_file.write("\n Label for cluster"+str(cluster_no+1))
            str1 = ' '.join(word for word in i)
            entity_recognier_list=self.get_continuous_chunks(str1)
            cluster_no=cluster_no+1
            for i in range(0,3):
                output_file.write("\n"+str(entity_recognier_list[i]))


def main():

    query_name="Alzheimer's Drug Treatment"
    resdocs = result_clustering.search(result_clustering.es,query_name, 100)

    # clustering search results
    centroid=result_clustering.results_clustering(resdocs)

    # labelling clusters based on words appeared in the query results
    result_clustering.label_for_cluster(centroid,query_name)



if __name__ == '__main__':
    main()