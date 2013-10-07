var async = require('async');
var config = require('config');
var mysql = require('./lib/mysql')(config.db);
var redis = require('./lib/redis');

var buildQuery = function(nextParams) {
  var query = 'SELECT * FROM entry_sets';
  if (nextParams) {
    var params = nextParams.split(',');
    var id = params[0];
    var updated_at = mysql.formatDate(params[1]);
    query += " WHERE";
    query += " updated_at > '" + updated_at + "'";
    query += " OR (updated_at = '" + updated_at + "' AND id > " + id + ")";
  }
  query += ' ORDER BY updated_at, id';
  query += ' LIMIT 10';
  return query;
};

var nextChunk = function(callback) {
  // get the next batch
  redis.compare_and_swap('entry_set_updater', function(err, nextParams, updateParams) {
    var query = buildQuery(nextParams);
    // exec query
    mysql.db.query(query, function(err, rows) {
      if (err) return callback(err);
      if (rows.length) {
        // update the next params
        var last = rows[rows.length - 1];
        updateParams(last.id + ',' + last.updated_at, function(err) {
          if (err) return callback(err); // someone else is processing this block
          // update the index
          updateIndex(rows, callback);
        });
      }
    });
  });
};

module.exports = nextChunk;

// elastic search
var es = require('node-elasticsearch').weheartit.addType('entry_set');

var updateIndex = function(entrySets, callback) {
  console.log('updating', entrySets.length, 'entry sets');
  async.each(entrySets, function(entrySet, done) {
    var searchable = {
      _id: entrySet.id,
      name: entrySet.name,
      setteds_count: entrySet.setteds_count,
      updated_at: entrySet.updated_at
    };
    es.entry_set.add(searchable, done);
  }, callback);
};

