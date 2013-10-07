var redis = require('redis');
var client = module.exports = redis.createClient();

module.exports = {
  client: client,
  compare_and_swap: function(key, callback) {
    client.watch(key, redis.print);
    client.get(key, function(err, value) {
      if (err) return callback(err);
      callback(null, value, function(value, done) {
        client.multi()
          .set(key, value)
          .exec(done);
      });
    });
  }
};
