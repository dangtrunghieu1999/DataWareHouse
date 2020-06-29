package hieu;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;


public class LoadStagingToWareHouse extends Constant {
	public static void load(Config conf, Log log) throws SQLException {

		System.out.println("Start load data from " + conf.getSrcFeed());
		long start = System.currentTimeMillis();

		//  Lấy các thuộc tính từ Configuration
		String propsSTForWH = conf.getPropsStagingForWareHouse().trim();
		String propsOfWH = conf.getPropsWarehouse().trim();
		String tableST = conf.getTableStaging().trim();
		int srcId = conf.getIdConf();
		String srcFeed = conf.getSrcFeed();
		String[] tmpProps = propsSTForWH.split(",");
		int numOfRecords = 0;

		boolean success = false;

		//  Tạo query GetAllDataFromStaging
		String sqlGetAllDataFromStaging = "SELECT ID_ST," + propsSTForWH + " FROM " + tableST;

		//  Tạo Connection
		MySQLConnection mySQLConn = new MySQLConnection();

		//  Kết nối đến STAGING
		Connection stConn = mySQLConn.getConn(URL_STAGING);

		//  Thực thi query GetAllDataFromStaging
		ResultSet rs = stConn.createStatement().executeQuery(sqlGetAllDataFromStaging);

		//  Kết nối đến WAREHOUSE
		Connection whConn = mySQLConn.getConn(URL_WAREHOUSE);

		String tmp = tmpProps[0].trim();
		if (tmp.startsWith("`")) {
			tmp = tmp.substring(1, tmp.length() - 1);
		}

		//  Duyệt ResultSet rs
		while (rs.next()) {

			String idSK = rs.getString(tmp);

			//  Tạo query kiểm tra xem sinh viên có tồn tại trong WAREHOUSE
			// hay không
			String sqlCheckExistStudent = "SELECT * FROM repository WHERE SOURCE_ID=" + srcId + " AND MA_SINH_VIEN='"
					+ idSK + "' AND IS_ACTIVE=1";

			//  Thực thi query CHECK EXIST
			ResultSet rsCheckStudent = whConn.createStatement().executeQuery(sqlCheckExistStudent);

			//  Kiểm tra xem sinh viên có tồn tại trong WAREHOUSE
			if (rsCheckStudent.next()) {
				// Tạo query UPDATE Record trong WAREHOUSE
				String sqlUpdate = "UPDATE repository SET IS_ACTIVE=0 " + "WHERE SOURCE_ID=" + srcId
						+ " AND MA_SINH_VIEN='" + idSK + "' AND IS_ACTIVE=1";
				System.out.println(sqlUpdate);
				whConn.createStatement().executeUpdate(sqlUpdate);
			}

			// Tạo Query INSERT Record vào trong WAREHOUSE
			String sqlInsert = "INSERT INTO WAREHOUSE.repository(ID_SK, " + propsOfWH
					+ ", SOURCE_ID, SOURCE_NAME, TIME_IMPORT, IS_ACTIVE)" + " SELECT '" + new Date().getTime() + "',"
					+ propsSTForWH + "," + srcId + ",'" + srcFeed + "','" + "',1"
					+ " FROM STAGING." + tableST + " WHERE ID_ST=" + rs.getInt("ID_ST");

			// Thực thi query INSERT
			numOfRecords += whConn.createStatement().executeUpdate(sqlInsert);

			if (numOfRecords == 0) {
				success = false;
			}
		}

		// Truncate
		stConn.createStatement().executeUpdate("TRUNCATE TABLE STAGING." + tableST);
		
		// Dong ket noi STAGING va WAREHOUSE
		mySQLConn.close(stConn);
		mySQLConn.close(whConn);

	}
	
	
	public static void main(String[] args) throws SQLException {
		MySQLConnection mySQLConn = new MySQLConnection();
		List<Config> lstConf = mySQLConn.loadAllConfs();
		String fileToStaging = Action.FILE_TO_STAGING.name().toLowerCase();
		String success = Result.SUCCESS.name().toLowerCase();

		System.out.println("LOAD DATA FROM STAGING TO WAREHOUSE\n");
		for (Config conf : lstConf) {
			String sqlLoadSTToWH = "SELECT * FROM data_file_log WHERE ID_CONF=" + conf.getIdConf()
			+ " AND ACTION_TYPE='" + fileToStaging + "' AND LOG_STATUS='" + success + "'";
			List<Log> lstLog = mySQLConn.getAllLogByCondition(sqlLoadSTToWH);
			if (lstLog.size() > 0) {
				for (Log log : lstLog) {
					LoadStagingToWareHouse.load(conf, log);
				}
			}
		}
		
		
	}
}
