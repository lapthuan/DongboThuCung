require("dotenv").config();
const sql = require("mssql");

const sqlConfig = {
  user: process.env.SQL_USER,
  password: process.env.SQL_PASSWORD,
  server: process.env.SERVER_NAME,
  database: process.env.SQL_DATABASE,
  requestTimeout: 600000,
  options: {
    encrypt: false,
  },
};

const sqlPool = new sql.ConnectionPool(sqlConfig);

const connectSQL = async () => {
  try {
    await sqlPool.connect();
    console.log("Kết nối thành công Sql Server!");
  } catch (error) {
    console.log("Lỗi kết nối SQL Server: " + error);
  }
};
module.exports = { sqlPool, connectSQL };
