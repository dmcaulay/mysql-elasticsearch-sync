var async = require('async');
var mysql = require('./mysql');
var redis = require('./redis');
var _ = require('underscore');

// process the next chunk of entry sets
var nextChunk = function(callback) {
  // get the next batch
  redis.compare_and_swap('entry_set_updater', function(err, nextParams, updateParams) {
    mysql.nextIds("entry_sets", nextParams, function(err, rows) {
      if (err || !rows.length) return callback(err);
      // update the next params
      var last = rows[rows.length - 1];
      updateParams(last.id + ',' + last.updated_at, function(err) {
        if (err) return callback(err); // someone else is processing this block
        // update the index
        async.each(rows, add, callback)
      });
    });
  });
};

var getTags = function(entrySets, callback) {
  entrySets = [{id: 32, setteds_count: 4},{id: 33, setteds_count: 4},{id: 34, setteds_count: 4}];
  var setIds = _.pluck(entrySets, "id").join(',');
  var entryQuery = 'SELECT entry_id, entry_set_id FROM setteds WHERE entry_set_id IN (' + setIds + ')';
  mysql.db.query(entryQuery, function(err, entryIds) {
    if (err || !entryIds.length) return callback(err, entrySets);
    var entryIdsList = _.pluck(entryIds, "entry_id").join(',');
    var tagQuery = 'SELECT entry_id, tag_id, tags.name AS tag_name FROM tagged JOIN tags ON tags.id = tagged.tag_id WHERE entry_id IN (' + entryIdsList + ')';
    mysql.db.query(tagQuery, function(err, tags) {
      if (err || !tags.length) return callback(err, entrySets);
      callback(err, setTags(entrySets, entryIds, tags));
    });
  });
};

var setTags = function(entrySets, entryIds, tags) {
  var entryToSets = _.groupBy(entryIds, "entry_id");
  var idToSet = _.groupBy(entrySets, "id");

  // add tags to entry sets
  tags.forEach(function(tag) {
    var sets = entryToSets[tag.entry_id];
    sets.forEach(function(set) {
      var entrySet = idToSet[set.entry_set_id][0];
      entrySet.tags = entrySet.tags || {};
      if (!entrySet.tags[tag.tag_name]) entrySet.tags[tag.tag_name] = 0;
      entrySet.tags[tag.tag_name] += 1;
    });
  });

  entrySets.forEach(function(es) {
    // filter tags
    es.tags = Object.keys(es.tags).map(function(tag) {
      if (es.tags[tag]/es.setteds_count > 0.25) return { name: tag, count: es.tags[tag] }
      return false;
    }).filter(function(tag) { return tag; });
    // sort and limit
    es.tags = _.sortBy(es.tags, function(tag) { return -tag.count; }).slice(0, 6);
  });

  return entrySets;
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
  search: search,
  getTags: getTags
};
