from flask import Flask
import simplejson as json
import os

app = Flask(__name__)

@app.route("/")
def hello():
    return os.environ.get('MONGOHQ_URL')

if __name__ == "__main__":
    app.run(debug=True)