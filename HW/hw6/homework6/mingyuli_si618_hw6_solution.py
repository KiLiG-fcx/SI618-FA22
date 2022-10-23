#!/usr/bin/python

'''
Created by hkarthik@umich.edu

'''

import json
from pyspark import SparkContext


def get_author_category_and_ratings(data):
    
    author_category_and_ratings_list = []

    # your code here
    ratings = data.get('review/score', None)
    authors = data.get('authors', None)
    categories = data.get('categories', None)
    user_id = data.get('User_id', None)
    
    active_user = 0
    inactive_user = 0
    # active/inactive count
    if user_id:
        active_user = 1
    else:
        inactive_user = 1
        
    
    if authors and categories:
        for author in authors.split(','):
            for category in categories.split(','):
                author = author.strip()
                category = category.strip()
                if (author != "" and category != ""):
                    author_category_and_ratings_list.append(((author, category), (ratings, 1, active_user, inactive_user)))
    elif authors and (categories is None):
        for author in authors.split(','):
            author = author.strip()
            if (author != ""):
                author_category_and_ratings_list.append(((author, 'Unknown'), (ratings, 1, active_user, inactive_user)))
    elif (authors is None) and categories:
        for category in categories.split(','):
            category = category.strip()
            if (category != ""):
                author_category_and_ratings_list.append((('Unknown', category), (ratings, 1, active_user, inactive_user)))
    else:
        author_category_and_ratings_list.append((('Unknown', 'Unknown'), (ratings, 1, active_user, inactive_user)))
        
    return author_category_and_ratings_list

    # hint: author_category_and_ratings_list contains tuples with rating scores, number of ratings, and
    #       number of ratings from active and inactive users for each author-category combination



if __name__ == "__main__":


    sc = SparkContext(appName="pyspark_si618f22_avg_rating_score_per_category_per_author")

    input_file = sc.textFile("/scratch/siads618f22_class_root/siads618f22_class/shared_data/hw6_data/book_ratings_data_jsonl.json")

    solution =  input_file.map(lambda line: json.loads(line))\
            .filter(lambda line: (line['review/summary'] == None) or \
                    ((float(line['review/helpfulness'].split('/')[1])!=0)\
                    and (float(line['review/helpfulness'].split('/')[0])/float(line['review/helpfulness'].split('/')[1])>=0.25)))\
                .flatMap(get_author_category_and_ratings)\
                .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3]))\
                .map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1], x[1][2], x[1][3])))\
                .sortBy(lambda x: x[0][1], ascending=True)\
                .sortBy(lambda x: x[1][1], ascending=False)\
                .sortBy(lambda x: x[0][0], ascending=True)\
                .map(lambda x: x[0][0]+'\t'+x[0][1]+'\t'+str(round(x[1][0],2))+'\t'+str(x[1][1])+'\t'+str(x[1][2])+'\t'+str(x[1][3]))

    solution.collect()
    solution.saveAsTextFile('mingyuli_si618_hw6_output')
