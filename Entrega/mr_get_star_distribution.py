
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import json
import re
import itertools

from mr3px.csvprotocol import CsvProtocol

def get_json_element(line):
    ''' Had to create this function because my shrinked JSON were dumped as a list,
    not as various JSON Objects as in the original files, so this function
    handles both cases (multiple JSON objects and a LIST of JSON Objects) '''
    line = json.loads(line)
    if isinstance(line, list):
        for real_json_object in line:
            yield real_json_object
    else:
        yield line

BUSINESS_CATEGORIES = {}
class TopReviewerByCategory(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV

    def mapper_user_reviews_by_cat(self, _, line):
        line = line.split(';')
        stars = int(line[2])
        category = line[1]
        star_array = [0,0,0,0,0]
        star_array[stars-1] = 1
        yield [category, star_array]

    def ratings_per_category_reducer(self, key, values):
        yield key, list(values)

    def total_ratings_per_category_reducer(self, key, values):
        for value in values:
            category = key
            stars = [str(sum(x)) for x in zip(*value)]
            yield "STARS", [category] + stars
                  
    def steps(self):
        return [MRStep(mapper=self.mapper_user_reviews_by_cat, reducer=self.ratings_per_category_reducer),
                MRStep(reducer=self.total_ratings_per_category_reducer)]

if __name__ == '__main__':
    TopReviewerByCategory.run()