require("dotenv").config();
const mysql = require("mysql2");

const mysqlConfig = {
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
};
const mysqlConnection = mysql.createConnection(mysqlConfig);

const connectMysql = () => {
  mysqlConnection.connect((err) => {
    if (err) {
      console.error("Lỗi kết nối Mysql:", err.message);
    } else {
      console.log("Kết nối Mysql thành công!");
    }
  });
};

module.exports = { mysqlConnection, connectMysql };
