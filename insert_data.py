from pymongo import *
import os
import datetime
import simplejson as json

MONGO_URL = os.environ.get('MONGOHQ_URL')

if MONGO_URL:
    # Get a connection
    conn = pymongo.Connection(MONGO_URL)
    
    # Get the database
    db = conn[urlparse(MONGO_URL).path[1:]]
    states = db.states

print "Test sucessful"

def weird_csv_to_dict(file):
	for line in file:
		if line == 'w_geocode,h_geocode,S000,SA01,SA02,SA03,SE01,SE02,SE03,SI01,SI02,SI03,createdate':
			continue
		else:
			line_dict = {}
			line = line.split(',')
			line_dict[w_geocode] = line[0]
			line_dict[h_geocode] = line[1]
			line_dict[S000] = line[2]
			line_dict[SA01] = line[3]
			line_dict[SA02] = line[4]
			line_dict[SA03] = line[5]
			line_dict[SE01] = line[6]
			line_dict[SE02] = line[7]
			line_dict[SE03] = line[8]
			line_dict[SI01] = line[9]
			line_dict[SI02] = line[10]
			line_dict[SI03] = line[11]
			line_dict[createdate] = datetime.date(int(line[12][0:4]),int(line[12][4:6]),int(line[12][6:8]))
			state_id = states.insert(line_dict)