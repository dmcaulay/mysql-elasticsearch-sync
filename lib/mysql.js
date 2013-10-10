var mysql = require('mysql');
var moment = require('moment');

var connection;
module.exports = function(config) {
  if (!connection) {
    connection = mysql.createConnection(config);
    connection.connect();
  }
  return {
    db: connection,
    formatDate: function(date) {
      return moment(date).format('YYYY-MM-DD HH:mm:ss');
    }
  };
};
