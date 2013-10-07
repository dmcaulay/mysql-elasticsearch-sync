var redis = require('redis');
var client = module.exports = redis.createClient();

module.exports = {
  client: client,
  get_and_watch: function(key, callback) {
    client.watch(key, redis.print);
    client.get(key, callback);
  },
  set_watched: function(key, value, callback) {
    client.multi()
      .set(key, value)
      .exec(callback);
  }
};
