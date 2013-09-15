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
	h_geocode 	= Column(String,nullable=False)
	w_geocode 	= Column(String,nullable=False)
 	s000      	= Column(Integer) 
 	sa01      	= Column(Integer) 
 	sa02      	= Column(Integer) 
 	sa03	  	= Column(Integer) 
	se01	  	= Column(Integer) 
 	se02	  	= Column(Integer) 
 	se03	  	= Column(Integer) 
 	si01	  	= Column(Integer) 
 	si02	  	= Column(Integer) 
 	si03	  	= Column(Integer)  
 	createdate 	= Column(Date)    
 	data_year 	= Column(Integer, nullable=False) 
 	job_type  	= Column(String, nullable=False)
