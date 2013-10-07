var config = require('config');
var mysql = require('./lib/mysql')(config.db);
var moment = require('moment')
var redis = require('./lib/redis');

var nextChunk = function(callback) {
  redis.get_and_watch('entry_set_updater', function(err, lastQuery) {
    var query = 'SELECT * FROM entry_sets';
    if (lastQuery) {
      var params = lastQuery.split(',');
      var id = params[0];
      var updated_at = mysql.formatDate(params[1]);
      query += " WHERE updated_at > '" + updated_at + "' OR (updated_at = '" + updated_at + "' AND id > " + id + ")";
    }
    query += ' ORDER BY updated_at, id';
    query += ' LIMIT 10';
    console.log('query', query);
    mysql.db.query(query, function(err, rows) {
      if (err) callback(err);

      console.log('processing:', rows);
      if (rows.length == 10) {
        var last = rows[rows.length - 1];
        redis.set_watched('entry_set_updater', last.id + ',' + last.updated_at, callback);
      }
    });
  });
};

var chunkComplete = function(err) {
  console.log('done','error',err);
};

nextChunk(chunkComplete);
