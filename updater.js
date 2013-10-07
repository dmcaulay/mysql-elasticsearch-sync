var config = require('config');
var nextChunk = require('./lib/entrySets');

// setup elasticsearch
require('node-elasticsearch').config(config.search);

var chunkComplete = function(err) {
  console.log('done','error',err);
};

nextChunk(chunkComplete);
