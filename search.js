var config = require('config');
require('node-elasticsearch').config(config.search);
var searchEntrySets = require('./lib/entrySet').search;

searchEntrySets("love", function(err, entrySets) {
  console.log('res',err,entrySets);
});
