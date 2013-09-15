from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
import os
import datetime

# This configures the database. You need to set the env variable DB_HOST for it to work. 
# the db host should be the url of 
DB_HOST = os.environ.get('DB_HOST')
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://cenus@%s/census' % DB_HOST
db = SQLAlchemy(app)

class OriginDestination(db.Model):
	__tablename__ = 'origin_destination'
	h_geocode 	= db.Column(String(15),nullable=False)
	w_geocode 	= db.Column(String(15),nullable=False)
 	s000      	= db.Column(Integer) 
 	sa01      	= db.Column(Integer) 
 	sa02      	= db.Column(Integer) 
 	sa03	  	= db.Column(Integer) 
	se01	  	= db.Column(Integer) 
 	se02	  	= db.Column(Integer) 
 	se03	  	= db.Column(Integer) 
 	si01	  	= db.Column(Integer) 
 	si02	  	= db.Column(Integer) 
 	si03	  	= db.Column(Integer)  
 	createdate 	= db.Column(Date)    
 	data_year 	= db.Column(Integer, nullable=False) 
 	job_type  	= db.Column(String(255), nullable=False)

print OriginDestination.query.limit(1).all()
