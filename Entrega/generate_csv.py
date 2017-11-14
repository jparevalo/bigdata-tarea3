
import json
import re
import itertools
import sys

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
business = sys.argv[1]
reviews = sys.argv[2]
out_name = sys.argv[3]
files = [business, reviews]
out_file = open(out_name, "w")
for f in files:
    opened_file = open(f,"r")
    for line in opened_file:
        for l in get_json_element(line):
            if 'categories' not in l:
                if l['business_id'] in BUSINESS_CATEGORIES:
                    categories = BUSINESS_CATEGORIES[l['business_id']]
                    for category in categories:
                        review_id = l['review_id']
                        stars = str(l['stars'])
                        out_file.write(review_id+';'+category+';'+stars+ '\n')
            else:
                BUSINESS_CATEGORIES[l['business_id']] = l['categories']
out_file.close()