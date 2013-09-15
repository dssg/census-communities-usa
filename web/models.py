from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
import os
import datetime

# This configures the database. You need to set the env variable DB_HOST for it to work. 
# the db host should be the url of 
DB_HOST = os.environ.get('DB_HOST')
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://census@%s/census' % DB_HOST
db = SQLAlchemy(app)

class OriginDestination(db.Model):
	__tablename__ = 'origin_destination'
	h_geocode 	= db.Column(db.String(15),nullable=False,primary_key=True)
	w_geocode 	= db.Column(db.String(15),nullable=False,primary_key=True)
 	s000      	= db.Column(db.Integer) 
 	sa01      	= db.Column(db.Integer) 
 	sa02      	= db.Column(db.Integer) 
 	sa03	  	= db.Column(db.Integer) 
	se01	  	= db.Column(db.Integer) 
 	se02	  	= db.Column(db.Integer) 
 	se03	  	= db.Column(db.Integer) 
 	si01	  	= db.Column(db.Integer) 
 	si02	  	= db.Column(db.Integer) 
 	si03	  	= db.Column(db.Integer)  
 	createdate 	= db.Column(db.Date)    
 	data_year 	= db.Column(db.Integer, nullable=False,primary_key=True) 
 	job_type  	= db.Column(db.String(255), nullable=False,primary_key=True)

print OriginDestination.query.limit(1).all()
