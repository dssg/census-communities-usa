import pymongo
from urlparse import urlparse
import os

MONGO_URL = 'mongodb://heroku:29e9df65f428be35535a2f6786508275@shannon.mongohq.com:10038/app16981083'

if MONGO_URL:
    # Get a connection
    print "Tester"
    conn = pymongo.Connection(MONGO_URL)
    print "print con"
    # Get the database
    db = conn[urlparse(MONGO_URL).path[1:]]
    states = db.states
    test = states.find_one()
    print test
else:
	print "test failed"

print "Test sucessful"