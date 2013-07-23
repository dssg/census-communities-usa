census-communities-usa
======================

Mapping and analyzing local business data from the Census Bureau

This the repo for the API that exposes [LODES](http://lehd.did.census.gov/onthemap/LODES7/LODESTechDoc7.0.pdf) via a JSON api.

The JSON API is served via a flask app. A copy is currently living on [heroku](http://enigmatic-fjord-3697.herokuapp.com/). The database is [MongoDB](http://mongodb.com/). The API and its contents live in `/web`.

You can see an example of each item in the collection in `/data_notes/example_data.json`. The data notes folder contains info on the dataset. 

The scrapers are live in `/scrapers`. 

Each datapoint represents a census block. 

We would like to build an API that allows you to query the census data without having to download entire CSVs. 