var express = require('express'),
    od = require('./routes/origin_destination');
 
var app = express();
 
app.get('/od', od.findAll);
app.get('/od/:id', od.findById);
app.get('/od/:state',od.findByState)
 
app.listen(3000);
console.log('Listening on port 3000...');