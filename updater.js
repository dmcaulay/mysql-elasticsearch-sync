var config = require('config');
require('node-elasticsearch').config(config.search);
var updateEntrySets = require('./lib/entrySet').nextChunk;

// setup elasticsearch
require('node-elasticsearch').config(config.search);

updateEntrySets(function(err) {
  console.log('done','error',err);
});
