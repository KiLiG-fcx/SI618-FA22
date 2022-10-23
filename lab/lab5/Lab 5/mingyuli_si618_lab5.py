'''
created by hkarthik.
'''
from operator import countOf
from mrjob.job import MRJob
import mrjob

class UpvotesAverageCalculator(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol

    def mapper(self, _, line):
        try:
            # # +++your code here+++
            date = line.split('\t')[0]# date
            description = line.split('\t')[1:(len(line.split('\t'))-1)]# string containing all category tags
            upvotes = line.split('\t')[-1]# upvotes
            tags = []
            if description is not None:
                # # +++your code here+++
                tags = description[0].split(',')# get a list of all the tags from the description
                
                for tag in tags:
                    if (tag.rstrip()!=""):
                        yield str(date)+"\t"+tag.strip().replace("'",""),int(upvotes)
                # hint: the mapper maps each category under a date with the upvotes and the product count'''
        except:
            pass
    
    def combiner(self, key, values):
        # # +++your code here+++
        upvotes=0
        counts=0
        for i in values:
            upvotes += i
            counts += 1
        yield key, (upvotes,counts)
        # hint: the combiner combines to get the total upvotes and the total product counts

    def reducer(self, key, values):
        # # +++your code here+++
        total = 0
        num = 0
        for i in values:
            total += i[0]
            num += i[1]
        yield key, str(round(total/num,2))
        # the reducer combines and reduces the total upvotes and total product counts to the average number of upvotes per product


if __name__ == '__main__':
    UpvotesAverageCalculator.run()