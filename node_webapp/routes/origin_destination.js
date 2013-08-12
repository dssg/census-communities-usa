var mongo = require('mongodb');
 
var Server = mongo.Server,
    Db = mongo.Db,
    BSON = mongo.BSONPure;

var server = new Server('ec2-54-214-169-9.us-west-2.compute.amazonaws.com', 27017, {auto_reconnect: true});
db = new Db('census', server);

db_collection = 'origin_destination'

db.open(function(err, db) {
    if(!err) {
        console.log("Connected to 'census' database");
        db.collection(db_collection, {strict:true}, function(err, collection) {
            if (err) {
                console.log("The 'origin_destination' collection doesn't exist. Exiting Application");
                exit(1);
            }
        });
    }
});

exports.findAll = function(req, res) {
	db.collection(db_collection, function(err, collection) {
        collection.find().toArray(function(err, items) {
            res.send(items);
        });
    });
};
 

exports.findById = function(req, res) {
    var id = req.params.id;
    console.log('id: ' + id);
    db.collection(db_collection, function(err, collection) {
        collection.findOne({'_id':new BSON.ObjectID(id)}, function(err, item) {
            res.send(item);
        });
    });
};

exports.findByState = function(req, res) {
	var state = req.params.state;
	console.log('State: ' + state);
	db.collection(db_collection, function(err, collection) {
		collection.find({"origin_state":state}, function(err, item) {
			res.send(item);
		});
	});
};