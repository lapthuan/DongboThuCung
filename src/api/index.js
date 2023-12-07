require("dotenv").config();
const { connectSQL, sqlPool } = require("../config/connectSqlserver");
const { connectMysql, mysqlConnection } = require("../config/connectMysql");
const { connectOracle, executeOracleQuery } = require("../config/connectOracle");
const { connectPostgres, postgresClient } = require("../config/connectPostgresql");


connectSQL();
connectMysql();
connectOracle();
connectPostgres();

async function DongBoDuLieu() {
  await DongBoChiNhanh()
  await DongBoKho()
  await DongBoNhanVien()
  await XoaDongBoChiNhanh()
  await XoaDongBoNhanVien()
  await XoaDongBoKho();
}

setInterval(DongBoDuLieu, 10000)



async function XoaDongBoChiNhanh() {
  try {
    const mySqlCheckQuery = `SELECT * from chinhanh `;
    const [mySqlCheckResult] = await mysqlConnection.promise().query(mySqlCheckQuery);

    mySqlCheckResult.forEach(async (row) => {
      const { MaChiNhanh } = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM chinhanh WHERE machinhanh =  '${MaChiNhanh}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count == 0) {
        const mySqlCheckQuery = `delete from chinhanh where machinhanh = '${MaChiNhanh}' `;
        await mysqlConnection.promise().query(mySqlCheckQuery);

        console.log(`Xóa dữ liệu ${MaChiNhanh} đồng bộ Mysql`)
      }
    })

    const oracleCheckQuery = `SELECT * FROM chinhanh`;
    const oracleCheckResult = await executeOracleQuery(oracleCheckQuery);

    for (const row of oracleCheckResult.rows) {
      const [MaChiNhanh, TenChiNhanh, MaTinh] = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM chinhanh WHERE machinhanh =  '${MaChiNhanh}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count === 0) {
        const oracleDeleteQuery = `DELETE FROM chinhanh WHERE machinhanh = :1`;
        await executeOracleQuery(oracleDeleteQuery, [MaChiNhanh]);

        console.log(`Xóa dữ liệu ${MaChiNhanh} đồng bộ Oracle`);
      }
    }
    const pgCheckQuery = 'SELECT * FROM chinhanh';
    const pgCheckResult = await postgresClient.query(pgCheckQuery);

    for (const row of pgCheckResult.rows) {
      const { machinhanh } = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM chinhanh WHERE machinhanh =  '${machinhanh}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count === 0) {
        const pgDeleteQuery = 'DELETE FROM chinhanh WHERE machinhanh = $1';
        await postgresClient.query(pgDeleteQuery, [machinhanh]);

        console.log(`Xóa dữ liệu ${machinhanh} đồng bộ PostgreSQL`);
      }
    }
  } catch (error) {
    console.error('Error in XoaDongBoNhanVien:', error);
  }


}

async function XoaDongBoNhanVien() {
  try {
    // MySQL
    const mySqlCheckQuery = `SELECT * FROM nhanvien `;
    const [mySqlCheckResult] = await mysqlConnection.promise().query(mySqlCheckQuery);

    mySqlCheckResult.forEach(async (row) => {
      const { MaNhanVien } = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM nhanvien WHERE manhanvien = '${MaNhanVien}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count == 0) {
        const mySqlDeleteQuery = `DELETE FROM nhanvien WHERE manhanvien = ?`;
        await mysqlConnection.promise().query(mySqlDeleteQuery, [MaNhanVien]);

        console.log(`Xóa dữ liệu ${MaNhanVien} đồng bộ Mysql`);
      }
    })

    //Oracle
    const oracleCheckQuery = `SELECT * FROM nhanvien`;
    const oracleCheckResult = await executeOracleQuery(oracleCheckQuery);

    for (const row of oracleCheckResult.rows) {
      const [MaNhanVien, a, b, c] = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM nhanvien WHERE manhanvien = '${MaNhanVien}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count == 0) {
        const oracleDeleteQuery = `DELETE FROM nhanvien WHERE manhanvien = :1`;
        await executeOracleQuery(oracleDeleteQuery, [MaNhanVien]);

        console.log(`Xóa dữ liệu ${MaNhanVien} đồng bộ Oracle`);
      }
    }

    // PostgreSQL
    const pgCheckQuery = 'SELECT * FROM nhanvien';
    const pgCheckResult = await postgresClient.query(pgCheckQuery);

    for (const row of pgCheckResult.rows) {
      const { manhanvien } = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM nhanvien WHERE manhanvien = '${manhanvien}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count == 0) {
        const pgDeleteQuery = 'DELETE FROM nhanvien WHERE manhanvien = $1';
        await postgresClient.query(pgDeleteQuery, [manhanvien]);

        console.log(`Xóa dữ liệu ${manhanvien} đồng bộ PostgreSQL`);
      }
    }

  } catch (error) {
    console.error('Error in XoaDongBoNhanVien:', error);
    // Rollback transactions if applicable
  }
}

async function XoaDongBoKho() {
  try {
    // MySQL
    const mySqlCheckQuery = `SELECT * FROM kho `;
    const [mySqlCheckResult] = await mysqlConnection.promise().query(mySqlCheckQuery);

    mySqlCheckResult.forEach(async (row) => {
      const { MaKho } = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM kho WHERE makho = '${MaKho}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count == 0) {
        const mySqlDeleteQuery = `DELETE FROM kho WHERE makho = ?`;
        await mysqlConnection.promise().query(mySqlDeleteQuery, [MaKho]);

        console.log(`Xóa dữ liệu ${MaKho} đồng bộ Mysql`);
      }
    })

    // Oracle
    const oracleCheckQuery = `SELECT * FROM kho`;
    const oracleCheckResult = await executeOracleQuery(oracleCheckQuery);

    for (const row of oracleCheckResult.rows) {
      const [MaKho, a, b] = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM kho WHERE makho = '${MaKho}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count == 0) {
        const oracleDeleteQuery = `DELETE FROM kho WHERE makho = :1`;
        await executeOracleQuery(oracleDeleteQuery, [MaKho]);

        console.log(`Xóa dữ liệu ${MaKho} đồng bộ Oracle`);
      }
    }

    // PostgreSQL
    const pgCheckQuery = 'SELECT * FROM kho';
    const pgCheckResult = await postgresClient.query(pgCheckQuery);

    for (const row of pgCheckResult.rows) {
      const { makho } = row;

      const sqlServerQuery = `SELECT COUNT(*) as count FROM kho WHERE makho = '${makho}'`;
      const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
      const dataToInsert = sqlServerResult.recordset;

      if (dataToInsert[0].count == 0) {
        const pgDeleteQuery = 'DELETE FROM kho WHERE makho = $1';
        await postgresClient.query(pgDeleteQuery, [makho]);

        console.log(`Xóa dữ liệu ${makho} đồng bộ PostgreSQL`);
      }
    }

  } catch (error) {
    console.error('Error in XoaDongBoKho:', error);
    // Rollback transactions if applicable
  }
}



async function DongBoChiNhanh() {
  try {
    console.log('Chi nhánh...')

    const sqlServerQuery = "SELECT  chinhanh.* FROM chinhanh JOIN tinh t on t.matinh = chinhanh.matinh where t.matinh = 'CT5'";
    const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
    const dataToInsert = sqlServerResult.recordset;

    dataToInsert.forEach(async (row) => {
      const { MaChiNhanh, TenChiNhanh, MaTinh } = row;

      // Kiểm tra xem dữ liệu đã tồn tại trong MySQL
      const mySqlCheckQuery = `SELECT COUNT(*) as count FROM chinhanh WHERE machinhanh = '${MaChiNhanh}' `;
      const [mySqlCheckResult] = await mysqlConnection.promise().query(mySqlCheckQuery);

      if (mySqlCheckResult[0].count == 0) {
        const mySqlCheckQuery = `INSERT INTO chinhanh VALUES  ('${MaChiNhanh}','${TenChiNhanh}','${MaTinh}') `;

        await mysqlConnection.promise().query(mySqlCheckQuery);
        console.log(`-----------Đã đồng bộ ${MaChiNhanh} vào mysql `)
      } else {
        const mySqlCheckQuery = `
        UPDATE chinhanh 
        SET TenChiNhanh = '${TenChiNhanh}', MaTinh = '${MaTinh}'
        WHERE MaChiNhanh = '${MaChiNhanh}'
      `;

        await mysqlConnection.promise().query(mySqlCheckQuery);

      }


    })

    const sqlServerQuery2 = "SELECT  chinhanh.* FROM chinhanh JOIN tinh t on t.matinh = chinhanh.matinh where t.matinh = 'HN1'";
    const sqlServerResult2 = await sqlPool.request().query(sqlServerQuery2);
    const dataToInsert2 = sqlServerResult2.recordset;

    dataToInsert2.forEach(async (row) => {
      const { MaChiNhanh, TenChiNhanh, MaTinh } = row;

      const checkChiNhanh = await executeOracleQuery(
        `SELECT COUNT(*) as count FROM chinhanh WHERE machinhanh = '${MaChiNhanh}' `
      );

      if (checkChiNhanh.rows[0][0] == 0) {

        const insertQuery = "INSERT INTO chinhanh (MaChiNhanh, TenChiNhanh, MaTinh) VALUES (:1,:2,:3)";

        await executeOracleQuery(insertQuery, [
          MaChiNhanh, TenChiNhanh, MaTinh
        ]);
        console.log(`-----------Đã đồng bộ ${MaChiNhanh} vào Oracel `)
      } else {
        const updateQuery = `
        UPDATE chinhanh 
        SET TenChiNhanh = :1, MaTinh = :2
        WHERE MaChiNhanh = :3
      `;

        await executeOracleQuery(updateQuery, [
          TenChiNhanh, MaTinh, MaChiNhanh
        ]);

      }
    })

    const sqlServerQuery3 = "SELECT  chinhanh.* from chinhanh except SELECT  chinhanh.* FROM chinhanh JOIN tinh t on t.matinh = chinhanh.matinh where t.matinh in ('HN1','CT5')";
    const sqlServerResult3 = await sqlPool.request().query(sqlServerQuery3);
    const dataToInsert3 = sqlServerResult3.recordset;

    dataToInsert3.forEach(async (row) => {
      const { MaChiNhanh, TenChiNhanh, MaTinh } = row;

      // Kiểm tra xem dữ liệu đã tồn tại trong PostgreSQL với tham số
      const pgCheckQuery = 'SELECT COUNT(*) as count FROM chinhanh WHERE machinhanh = $1';
      const pgCheckResult = await postgresClient.query(pgCheckQuery, [MaChiNhanh]);

      if (pgCheckResult.rows[0].count == 0) {
        // Thêm vào PostgreSQL
        const pgInsertQuery = 'INSERT INTO chinhanh (machinhanh, tenchinhanh,matinh) VALUES ($1, $2,$3)';
        await postgresClient.query(pgInsertQuery, [MaChiNhanh, TenChiNhanh, MaTinh]);
        console.log(`-----------Đã đồng bộ ${MaChiNhanh} vào PostgreSql `)

      } else {
        const pgUpdateQuery = 'UPDATE chinhanh SET tenchinhanh = $1, matinh = $2 WHERE machinhanh = $3';
        await postgresClient.query(pgUpdateQuery, [TenChiNhanh, MaTinh, MaChiNhanh]);
      }
    })
  } catch (error) {
    console.error('Lỗi: ', error.message);
  }
}
async function DongBoKho() {
  try {
    console.log('Kho...')
    const sqlServerQueryCT5 = "SELECT kho.* FROM kho JOIN chinhanh ON chinhanh.machinhanh = kho.MaChiNhanh JOIN tinh t ON t.matinh = chinhanh.matinh WHERE t.matinh = 'CT5'";
    const sqlServerResultCT5 = await sqlPool.request().query(sqlServerQueryCT5);
    const dataToInsertCT5 = sqlServerResultCT5.recordset;

    dataToInsertCT5.forEach(async (row) => {
      const { MaKho, TenKho, MaChiNhanh, MaSanPham, SoLuong } = row;

      // Kiểm tra xem dữ liệu đã tồn tại trong MySQL
      const mySqlCheckQuery = 'SELECT COUNT(*) as count FROM kho WHERE makho = ?';
      const [mySqlCheckResult] = await mysqlConnection.promise().query(mySqlCheckQuery, [MaKho]);

      if (mySqlCheckResult[0].count == 0) {
        const mySqlInsertQuery = 'INSERT INTO kho (makho, tenkho, machinhanh, masanpham, soluong) VALUES (?, ?, ?, ?, ?)';
        await mysqlConnection.promise().query(mySqlInsertQuery, [MaKho, TenKho, MaChiNhanh, MaSanPham, SoLuong]);
        console.log(`-----------Đã đồng bộ ${MaKho} vào MySQL `);
      } else {
        const mySqlUpdateQuery = 'UPDATE kho SET tenkho = ?, masanpham = ?, soluong = ?,machinhanh = ? WHERE makho = ?  ';
        await mysqlConnection.promise().query(mySqlUpdateQuery, [TenKho, MaSanPham, SoLuong, MaChiNhanh, MaKho]);

      }
    });

    const sqlServerQueryHN1 = "SELECT kho.* FROM kho JOIN chinhanh ON chinhanh.machinhanh = kho.MaChiNhanh JOIN tinh t ON t.matinh = chinhanh.matinh WHERE t.matinh = 'HN1'";
    const sqlServerResultHN1 = await sqlPool.request().query(sqlServerQueryHN1);
    const dataToInsertHN1 = sqlServerResultHN1.recordset;

    dataToInsertHN1.forEach(async (row) => {
      const { MaKho, TenKho, MaChiNhanh, MaSanPham, SoLuong } = row;

      const checkKho = await executeOracleQuery('SELECT COUNT(*) as count FROM kho WHERE makho = :1', [MaKho]);
      if (checkKho.rows[0][0] == 0) {
        const insertQuery = "INSERT INTO kho (makho, tenkho, machinhanh, masanpham, soluong) VALUES (:1,:2,:3,:4,:5)";

        await executeOracleQuery(insertQuery, [
          MaKho, TenKho, MaChiNhanh, MaSanPham, SoLuong
        ]);

        console.log(`-----------Đã đồng bộ ${MaKho} vào Oracle `);
      } else {
        const updateQuery = `
        UPDATE kho 
        SET tenkho = :2, masanpham = :4, soluong = :5, machinhanh = :3
        WHERE makho = :1
      `;

        await executeOracleQuery(updateQuery, [TenKho, MaSanPham, SoLuong, MaChiNhanh, MaKho]);

      }
    });

    const sqlServerQueryExcept = "SELECT  kho.* from kho except SELECT kho.* FROM kho JOIN chinhanh ON chinhanh.machinhanh = kho.MaChiNhanh JOIN tinh t ON t.matinh = chinhanh.matinh WHERE t.matinh IN ('HN1','CT5')";
    const sqlServerResultExcept = await sqlPool.request().query(sqlServerQueryExcept);
    const dataToInsertExcept = sqlServerResultExcept.recordset;

    dataToInsertExcept.forEach(async (row) => {
      const { MaKho, TenKho, MaChiNhanh, MaSanPham, SoLuong } = row;

      // Kiểm tra xem dữ liệu đã tồn tại trong PostgreSQL với tham số
      const pgCheckQuery = 'SELECT COUNT(*) as count FROM kho WHERE makho = $1';
      const pgCheckResult = await postgresClient.query(pgCheckQuery, [MaKho]);

      if (pgCheckResult.rows[0].count == 0) {
        // Thêm vào PostgreSQL
        const pgInsertQuery = 'INSERT INTO kho (makho, tenkho, machinhanh, masanpham, soluong) VALUES ($1, $2, $3, $4, $5)';
        await postgresClient.query(pgInsertQuery, [MaKho, TenKho, MaChiNhanh, MaSanPham, SoLuong]);
        console.log(`-----------Đã đồng bộ ${MaKho} vào PostgreSQL `);
      } else {
        const pgUpdateQuery = 'UPDATE kho SET tenkho = $1, masanpham = $2, soluong = $3, machinhanh = $4 WHERE makho = $5';
        await postgresClient.query(pgUpdateQuery, [TenKho, MaSanPham, SoLuong, MaChiNhanh, MaKho]);

      }
    });
  } catch (error) {
    console.error('Lỗi: ', error.message);
  }
}
async function DongBoNhanVien() {
  try {
    console.log("Nhân viên...")
    const sqlServerQuery = `
      SELECT nv.*, cn.machinhanh, cn.tenchinhanh, t.matinh, t.tentinh
      FROM nhanvien nv
      JOIN chinhanh cn ON nv.machinhanh = cn.machinhanh
      JOIN tinh t ON cn.matinh = t.matinh
    `;
    const sqlServerResult = await sqlPool.request().query(sqlServerQuery);
    const dataToInsert = sqlServerResult.recordset;

    for (const row of dataToInsert) {
      const { MaNhanVien, machinhanh, TenNhanVien, DiaChi, matinh } = row;

      if (matinh === 'CT5') {

        const mySqlCheckQuery = 'SELECT COUNT(*) as count FROM nhanvien WHERE manhanvien = ?';
        const [mySqlCheckResult] = await mysqlConnection.promise().query(mySqlCheckQuery, [MaNhanVien]);

        if (mySqlCheckResult[0].count === 0) {
          const mySqlInsertQuery = 'INSERT INTO nhanvien (manhanvien, machinhanh, tennhanvien, diachi) VALUES (?, ?, ?, ?)';
          await mysqlConnection.promise().query(mySqlInsertQuery, [MaNhanVien, machinhanh, TenNhanVien, DiaChi]);
          console.log(`-----------Đã đồng bộ ${MaNhanVien} vào MySQL `);
        } else {
          const mySqlUpdateQuery = 'UPDATE nhanvien SET tennhanvien = ?, diachi = ?,machinhanh = ? WHERE manhanvien = ?';
          await mysqlConnection.promise().query(mySqlUpdateQuery, [TenNhanVien, DiaChi, machinhanh, MaNhanVien]);
        }

      } else if (matinh === 'HN1') {
        const checkNhanVien = await executeOracleQuery('SELECT COUNT(*) as count FROM nhanvien WHERE manhanvien = :1', [MaNhanVien]);

        if (checkNhanVien.rows[0][0] == 0) {
          const InsertQuery = `
          INSERT INTO nhanvien (manhanvien, machinhanh, tennhanvien, diachi) VALUES (:1, :2, :3, :4)
          `;
          await executeOracleQuery(InsertQuery, [MaNhanVien, machinhanh, TenNhanVien, DiaChi]);

          console.log(`-----------Đã đồng bộ ${MaNhanVien} vào Oracle `);
        } else {
          const updateQuery = `
            UPDATE nhanvien 
            SET tennhanvien = :2, diachi = :4 ,machinhanh = :3
            WHERE manhanvien = :1
          `;
          await executeOracleQuery(updateQuery, [TenNhanVien, DiaChi, machinhanh, MaNhanVien]);
        }
      }
      else {
        const pgCheckQuery = 'SELECT COUNT(*) as count FROM nhanvien WHERE manhanvien = $1';
        const pgCheckResult = await postgresClient.query(pgCheckQuery, [MaNhanVien]);

        if (pgCheckResult.rows[0].count === 0) {
          const pgInsertQuery = 'INSERT INTO nhanvien (manhanvien, machinhanh, tennhanvien, diachi) VALUES ($1, $2, $3, $4)';
          await postgresClient.query(pgInsertQuery, [MaNhanVien, machinhanh, TenNhanVien, DiaChi]);
          console.log(`-----------Đã đồng bộ ${MaNhanVien} vào PostgreSQL `);
        } else {
          const pgUpdateQuery = 'UPDATE nhanvien SET tennhanvien = $1, diachi = $2, machinhanh = $3 WHERE manhanvien = $4';
          await postgresClient.query(pgUpdateQuery, [TenNhanVien, DiaChi, machinhanh, MaNhanVien]);
        }
      }
    }
  } catch (error) {
    console.error('Lỗi: ', error.message);
  }
}

