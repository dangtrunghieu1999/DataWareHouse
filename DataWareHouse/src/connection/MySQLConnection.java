package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import constants.Constant;
import control.Config;

import log.Log;

public class MySQLConnection extends Constant {

	public Connection getConn(String url) {
		Connection conn = null;
		try {
			Class.forName(DRIVER);
			conn = DriverManager.getConnection(url, USER, PASS);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public Connection getClientConn(String url, String user, String pass) {
		Connection conn = null;
		try {
			Class.forName(DRIVER);
			conn = DriverManager.getConnection(url, user, pass);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	public void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public List<Config> loadAllConfs() throws SQLException {

		List<Config> outputs = new ArrayList<Config>();
		Connection conn = getConn(URL_CONTROL);
		String sqlSelectAllConf = "SELECT * FROM data_file_configuration WHERE IS_ACTIVE<>0";
		PreparedStatement ps = conn.prepareStatement(sqlSelectAllConf);
		ResultSet rs = ps.executeQuery();
		
		while (rs.next()) {
			Config conf = new Config();
			conf.setIdConf(rs.getInt("ID"));
			conf.setRemoteDir(rs.getString("REMOTE_DIR"));
			conf.setLocalDir(rs.getString("LOCAL_DIR"));
			conf.setHostName(rs.getString("HOST_NAME"));
			conf.setPort(rs.getInt("PORT"));
			conf.setUserHost(rs.getString("USER_HOST"));
			conf.setUserPass(rs.getString("USER_PASS"));
			conf.setColumsHostFeed(rs.getString("COLUMNS_HOST_FEED"));
			conf.setPropsStagingForWareHouse(rs.getString("PROPS_STAGING_FOR_WAREHOUSE"));
			conf.setPropsWarehouse(rs.getString("PROPS_WAREHOUSE"));
			conf.setFeedDelimiter(rs.getString("FEED_DELIM"));
			conf.setSrcFeed(rs.getString("SRC_FEED"));
			conf.setTableStaging(rs.getString("TABLE_STAGING"));
			conf.setDateFormat(rs.getString("DATE_FORMAT"));
			conf.setDateFormat(rs.getString("DATE_FORMAT"));
			outputs.add(conf);
		}
		close(conn);
		return outputs;
	}

	public List<Log> getAllLogByCondition(String sqlQuery) throws SQLException{
		List<Log> lstLogs = new ArrayList<Log>();
		Connection conn = getConn(URL_CONTROL);
		PreparedStatement ps = conn.prepareStatement(sqlQuery);
		ResultSet rs = ps.executeQuery();
		while(rs.next()){
			Log log  = new Log();
			log.setId(rs.getInt("ID"));
			log.setIdConfig(rs.getInt("ID_CONF"));
			log.setActionType(rs.getString("ACTION_TYPE"));
			log.setSourceFeed(rs.getString("SOURCE_FEED"));
			log.setLogStatus(rs.getString("LOG_STATUS"));
			log.setFeedName(rs.getString("FEED_NAME"));
			lstLogs.add(log);
		}
		close(conn);
		return lstLogs;
	}
}
