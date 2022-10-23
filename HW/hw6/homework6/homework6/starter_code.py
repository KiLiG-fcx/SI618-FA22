#!/usr/bin/python

'''
Created by hkarthik@umich.edu

'''

import json
from pyspark import SparkContext

sc = SparkContext(appName='')
input_file = sc.textFile("/scratch/siads618f22_class_root/siads618f22_class/shared_data/hw6_data/book_ratings_data_jsonl.json")

def get_author_category_and_ratings(data):
    
    author_category_and_ratings_list = []

    # your code here
    
    

    return author_category_and_ratings_list

    # hint: author_category_and_ratings_list contains tuples with rating scores, number of ratings, and
    #       number of ratings from active and inactive users for each author-category combination




if __name__ == "__main__":


    sc = SparkContext(appName="pyspark_si618f22_avg_rating_score_per_category_per_author")

    input_file = sc.textFile("/scratch/siads618f22_class_root/siads618f22_class/shared_data/hw6_data/book_ratings_data_jsonl.json")

    solution =  input_file.map(lambda line: json.loads(line)) # loading each row of the rdd as a json object from each line of the input file\
        .filter() # complete the filter() operation to filter rows based on review helpfulness and summary conditions\
            . # use an rdd operation to apply the get_author_category_and_ratings() functions on the filtered rows\
                . # use rdd operations to get the average rating scores and the total number of ratings and total number of ratings from active and inactive users \
                    .sort() # sort the rdd as required \
                        .map() # format the rdd rows in the desired tab separated format 

    solution.collect()
    solution.saveAsTextFile('uniqname_si618_hw6_output')
