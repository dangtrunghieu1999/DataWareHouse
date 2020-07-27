package b3_warehouse;

import connection.MySQLConnection;
import constants.Constant;
import control.Configuration;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import log.Log;
import log.MySQLLog;
import transform.DateTransform;

public class LoadStagingToWareHouse extends Constant {
	public LoadStagingToWareHouse() {
	}


	public static void load(Configuration conf, Log log) throws SQLException {
		System.out.println("Start load data from " + conf.getSrcFeed());
		long start = System.currentTimeMillis();
		String propsSTForWH = conf.getPropsStagingForWareHouse(); 
		String propsOfWH = conf.getPropsWarehouse(); 
		String tableST = conf.getTableStaging();
		int srcId = conf.getIdConf();
		String srcFeed = conf.getSrcFeed();
		String[] tmpProps = propsSTForWH.split(",");
		int numOfRecords = 0;
		String sqlGetAllDataFromStaging = "SELECT ID_ST," + propsSTForWH + " FROM " + tableST;
		MySQLConnection mySQLConn = new MySQLConnection();
		Connection stConn = mySQLConn.getConn("jdbc:mysql://127.0.0.1:3306/STAGING?useSSL=false");
		Connection whConn = mySQLConn.getConn("jdbc:mysql://127.0.0.1:3306/WAREHOUSE?useSSL=false");
		ResultSet rs = stConn.createStatement().executeQuery(sqlGetAllDataFromStaging);
		String tmp = tmpProps[0].trim();
		if (tmp.startsWith("`")) {
			tmp = tmp.substring(1, tmp.length() - 1);
		}

		while (rs.next()) {
			String idSK = rs.getString(tmp);
			String sqlCheckExistStudent = "SELECT * FROM repository WHERE SOURCE_ID=" + srcId + " AND STUDENT_ID='"
					+ idSK + "' AND IS_ACTIVE=1";
			ResultSet rsCheckStudent = whConn.createStatement().executeQuery(sqlCheckExistStudent);
			String sqlInsert;
			if (rsCheckStudent.next()) {
				sqlInsert = "UPDATE repository SET IS_ACTIVE=0 WHERE SOURCE_ID=" + srcId + " AND STUDENT_ID='" + idSK
						+ "' AND IS_ACTIVE=1";
				whConn.createStatement().executeUpdate(sqlInsert);
			}

			sqlInsert = "INSERT INTO WAREHOUSE.repository(ID_SK, " + propsOfWH
					+ ", SOURCE_ID, SOURCE_NAME, TIME_IMPORT, IS_ACTIVE)" + " SELECT '" + (new Date()).getTime() + "',"
					+ propsSTForWH + "," + srcId + ",'" + srcFeed + "','" + DateTransform.getCurrentDateStr() + "',1"
					+ " FROM STAGING." + tableST + " WHERE ID_ST=" + rs.getInt("ID_ST");
			numOfRecords += whConn.createStatement().executeUpdate(sqlInsert);
		}

		stConn.createStatement().executeUpdate("TRUNCATE TABLE STAGING." + tableST);
		(new MySQLLog()).logLoadToDataWarehouse(conf, log, numOfRecords);
		mySQLConn.close(stConn);
		mySQLConn.close(whConn);
		long end = System.currentTimeMillis();
		System.out.println("Finished load data from " + srcFeed + " | Time lost " + (end - start) + " ms with "
				+ numOfRecords + " records inserted into WAREHOUSE");
	}

	
}
