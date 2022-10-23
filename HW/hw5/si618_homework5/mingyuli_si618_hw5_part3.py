'''
created by hkarthik.
'''
from mrjob.job import MRJob
import mrjob
import re
from nltk.stem import *
from nltk.stem.porter import *
stemmer = PorterStemmer()

class CountWords(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol
    def mapper(self, _, line):
        try:
            # # +++your code here+++
            congress = line.split('\t')[4] # congress
            titlewords = line.split('\t')[2]
            matches = re.findall(r'\b[a-zA-Z]{4,}\b', titlewords)
            for word in matches:
                stemword = stemmer.stem(word).lower()
                yield congress+"\t"+stemword,1
        except:
            pass
    
    def combiner(self, key, values):
        # # +++your code here+++
        yield key, sum(values)
        # hint: the combiner combines to get the total upvotes and the total product counts

    def reducer(self, key, values):
        # # +++your code here+++
        yield key, str(sum(values))
        # the reducer combines and reduces the total upvotes and total product counts to the average number of upvotes per product


if __name__ == '__main__':
    CountWords.run()