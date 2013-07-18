from flask import Flask
import simplejson as json
import os
import sys

app = Flask(__name__)

@app.route("/state/<state>")
def get_state(state):
	MONGO_URL = os.environ.get('MONGOHQ_URL')
	if MONGO_URL:
		return "you wanted a state"
	else:
		return "no MONGO_URL"

@app.route("/year/<year>")
def get_year(year):
	return "you wanted a year"

@app.route("/get-all/<year>/<state>")
def get_year_and_state(year,state):
	return "you wanted both"

@app.route("/")
def hello():
    return "Hello World, again"

if __name__ == "__main__":
	app.run(debug=True)