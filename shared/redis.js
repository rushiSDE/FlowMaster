const redis = require('redis');

let client;

function getRedisClient() {
  if (!client) {
    client = redis.createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379' });
    client.on('error', (error) => {
      // Redis is optional for local unit testing; we only fail closed when used in production flows.
      console.warn('Redis warning:', error.message);
    });
  }

  return client;
}

module.exports = { getRedisClient };
