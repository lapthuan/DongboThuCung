require("dotenv").config();
const { Client } = require("pg");

const postgresConfig = {
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT,
};

const postgresClient = new Client(postgresConfig);

const connectPostgres = async () => {
  try {
    await postgresClient.connect();
    console.log("Kết nối thành công PostgreSQL!");
  } catch (error) {
    console.error("Lỗi kết nối PostgreSQL:", error);
  }
};

module.exports = { postgresClient, connectPostgres };
