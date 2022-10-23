'''
created by hkarthik.
'''
from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob
import re
from nltk.stem import *
from nltk.stem.porter import *
stemmer = PorterStemmer()

class CountWords(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word_stem)
        ]

    def mapper_get_words(self, _, line):
        try:
            # # +++your code here+++
            titlewords = line.split('\t')[2]
            matches = re.findall(r'\b[a-zA-Z]{4,}\b', titlewords)
            for word in matches:
                stemword = stemmer.stem(word)
                yield (len(stemword),stemword.lower()), 1
        except:
            pass
    
    def combiner_count_words(self, key, values):
        # # +++your code here+++
        yield key, sum(values)
        # hint: the combiner combines to get the total upvotes and the total product counts

    def reducer_count_words(self, key, values):
        # # +++your code here+++
        yield key[0], (sum(values),key[1])
        # the reducer combines and reduces the total upvotes and total product counts to the average number of upvotes per product

    def reducer_find_max_word_stem(self, key, values):
        max_word = max(values, key=lambda x:x[0])
        yield str(key), str(max_word[1])+"\t"+str(max_word[0])

if __name__ == '__main__':
    CountWords.run()