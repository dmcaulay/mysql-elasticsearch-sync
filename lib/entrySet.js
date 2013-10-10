var async = require('async');
var config = require('config');
var mysql = require('./mysql')(config.db);
var redis = require('./redis');

// mysql
var buildQuery = function(nextParams) {
  var query = 'SELECT * FROM entry_sets';
  // if (nextParams) {
  if (false) {
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

// process the next chunk of entry sets
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
          console.log("rows",rows);
          // update the index
          async.each(rows, add, callback)
        });
      }
    });
  });
};

// elastic search
var es = require('node-elasticsearch').weheartit.addType('entry_set', {
  "_source" : {"enabled": false},
  "_all": {"enabled": false},
  "_timestamp": {
      "enabled": true,
      "path": "updated_at"
  },
  "properties": {
    "name": {"type": "string", "omit_norms": true},
    "setteds_count": {"type": "integer"},
    "updated_at": {"type": "date", "format": "date_time||date_time_no_millis"}
  }
});

var add = function(entrySet, callback) {
  console.log('adding',entrySet);
  var searchable = {
    _id: entrySet.id,
    name: entrySet.name,
    setteds_count: entrySet.setteds_count,
    updated_at: entrySet.updated_at
  };
  es.entry_set.add(searchable, callback);
};

var search = function(query_string, callback) {
  var query = {
    query: {
      match_phrase_prefix: {
        name: query_string
      }
    },
    sort: [
      { updated_at: 'desc' }
    ]
  };
  es.entry_set.search(query, callback);
};

module.exports = {
  nextChunk: nextChunk,
  search: search
};
