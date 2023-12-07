const { mysqlConnection } = require("../config/connectMysql");
const { executeOracleQuery } = require("../config/connectOracle");
const { postgresClient } = require("../config/connectPostgresql");

const isTableExistMysql = async (tableName) => {
  try {
    const query = `SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`;
    const params = [mysqlConnection.config.database, tableName];

    const result = await mysqlConnection.promise().query(query, params);
    const count = result[0][0].count;

    return count > 0;
  } catch (error) {
    console.error("Lỗi khi kiểm tra sự tồn tại của bảng:", error);
    throw error;
  }
};

const isTableExistOra = async (tableName) => {
  try {
    const query = `SELECT COUNT(*) as count FROM USER_TABLES WHERE TABLE_NAME = :tableName`;
    const params = [tableName];

    const result = await executeOracleQuery(query, params);
    const count = result.rows[0].count;

    return count > 0;
  } catch (error) {
    console.error("Lỗi khi kiểm tra sự tồn tại của bảng:", error);
    throw error;
  }
};

const isTableExistPg = async (tableName) => {
  try {
    const query = `SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = $1`;
    const params = [tableName];

    const result = await postgresClient.query(query, params);
    const count = result.rows[0].count;

    return count > 0;
  } catch (error) {
    console.error("Lỗi khi kiểm tra sự tồn tại của bảng:", error);
    throw error;
  }
};
module.exports = { isTableExistMysql, isTableExistOra, isTableExistPg };
