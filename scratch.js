var config = require('config');
require('node-elasticsearch').config(config.search);
var mysql = require('./lib/mysql');
var entrySet = require('./lib/entrySet');

mysql.nextIds("entry_sets", null, function(err, rows) {
  entrySet.getTags(rows, function(err, tags) {
    console.log(err, JSON.stringify(tags));
  });
});
