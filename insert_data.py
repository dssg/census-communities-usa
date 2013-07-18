import pymongo
import os
import datetime
import simplejson as json
import sys
from urlparse import urlparse


def t_or_f(val):
    if int(val) == 1:
        return "True"
    else:
        return "False"

input_file = open(sys.argv[1])
print input_file
MONGO_URL = os.environ.get('MONGOHQ_URL')

if MONGO_URL:
    print "First if statement"
    # Get a connection
    conn = pymongo.Connection(MONGO_URL)
    
    # Get the database
    db = conn[urlparse(MONGO_URL).path[1:]]
    states = db.census

    for line in input_file:
        print line
        if line == 'w_geocode,h_geocode,S000,SA01,SA02,SA03,SE01,SE02,SE03,SI01,SI02,SI03,createdate\n':
            continue
        else:
            line_dict = {}
            line = line.split(',')
            line_dict["Workplace_Census_Block_Code"] = line[0]
            line_dict["Residence_Census_Block_Code"] = line[1]
            line_dict["Total_Jobs"] = line[2]
            line_dict["Jobs_Age_<=_29"] = line[3]
            line_dict["Jobs_Age_30_To_54"] = line[4]
            line_dict["Jobs_Age_>=_55"] = line[5]
            line_dict["Jobs_1250_Or_Less"] = line[6]
            line_dict["Jobs_1251_To_3333"] = line[7]
            line_dict["Jobs_More_Than_3333"] = line[8]
            line_dict["Jobs_Goods"] = line[9]
            line_dict["Jobs_Trade_Transportation_Utilities"] = line[10]
            line_dict["Jobs_Other"] = line[11]
            line_dict["Date"] = datetime.datetime(int(line[12][0:4]),int(line[12][4:6]),int(line[12][6:8]))
            state_id = states.insert(line_dict)
            print state_id

