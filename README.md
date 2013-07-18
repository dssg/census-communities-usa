census-communities-usa
======================

Mapping and analyzing local business data from the Census Bureau

This the repo for the API that exposes [LODES](http://lehd.did.census.gov/onthemap/LODES7/LODESTechDoc7.0.pdf) via a JSON api.

The JSON API is served via a flask app. A copy is currently living on [heroku](http://enigmatic-fjord-3697.herokuapp.com/). The database is [MongoDB](http://mongodb.com/).

The requests avalible are `/year`,`/state` & `/year/state`

`/state` returns a json list of all the records for a given state. The form of request is `/CA`,`/IL`, etc.

`/year` takes a numerical year 2002 to 2011 and returns a json list for all the records in the year. 

`/year/state` returns the records for a given state and given year. 

The base unit of reuturn datepoint, and each query will return a list of datapoints..

An example data pount can be found in… [TODO]