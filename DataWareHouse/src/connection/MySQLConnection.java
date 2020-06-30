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
import control.Configuration;
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
			conf.setUserHost(rs.getString("USER_NAME"));
			conf.setUserPass(rs.getString("USER_PASS"));
			conf.setPropsStagingForWareHouse(rs.getString("PROPS_STAGING_FOR_WAREHOUSE"));
			conf.setPropsWarehouse(rs.getString("PROPS_WAREHOUSE"));
			conf.setFeedDelimiter(rs.getString("FEED_DELIM"));
			conf.setSrcFeed(rs.getString("SRC_FEED"));
			conf.setTableStaging(rs.getString("TABLE_STAGING"));
			
			outputs.add(conf);
		}
		close(conn);
		return outputs;
	}

	public static void main(String[] args) {
//		MySQLConnection sql = new MySQLConnection();
//		Connection c = sql.getConn(URL_STAGING);
//		if(sql !=null){
//			System.out.println("success");
//		}else{
//			System.out.println("failer");
//		}
		 System.out.println(new MySQLConnection().getConn(URL_CONTROL));
	}
}
