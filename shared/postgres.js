const { Pool } = require('pg');

let pool;

function getPool() {
  if (!pool) {
    pool = new Pool({
      host: process.env.POSTGRES_HOST || 'localhost',
      port: Number(process.env.POSTGRES_PORT || 5432),
      database: process.env.POSTGRES_DB || 'flowmaster',
      user: process.env.POSTGRES_USER || 'flowmaster',
      password: process.env.POSTGRES_PASSWORD || 'flowmaster',
      max: 10,
      idleTimeoutMillis: 30000
    });
  }

  return pool;
}

module.exports = { getPool };
