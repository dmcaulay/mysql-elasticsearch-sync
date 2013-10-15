var config = require('config');
var mysql = require('mysql');
var moment = require('moment');

var connection = mysql.createConnection(config.db);

var formatDate = function(date) {
  return moment(date).format('YYYY-MM-DD HH:mm:ss');
};

var nextIdsQuery = function(table, last) {
  var query = 'SELECT id, updated_at FROM ' + table;
  // if (last) {
  if (false) {
    var params = last.split(',');
    var id = params[0];
    var updated_at = formatDate(params[1]);
    query += " WHERE";
    query += " updated_at > '" + updated_at + "'";
    query += " OR (updated_at = '" + updated_at + "' AND id > " + id + ")";
  }
  query += ' ORDER BY updated_at, id';
  query += ' LIMIT 10';
  return query;
};

var nextIds = function(table, last, callback) {
  var query = nextIdsQuery(table, last);
  connection.query(query, callback);
};

module.exports =  {
  db: connection,
  nextIds: nextIds
}
