package hieu;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;



public class MySQLLog extends Constant implements Ilog{

	@Override
	public void logLoadToDataWarehouse(Config conf, Log log, int numOfRecords, boolean success) throws SQLException {
		// TODO Auto-generated method stub
		MySQLConnection mySQLConn = new MySQLConnection();
		Connection conn = mySQLConn.getConn(URL_CONTROL);
		
		String query = "UPDATE data_file_log SET ACTION_TYPE=?, LOG_STATUS=?, TIME_LOAD_WAREHOUSE=?,ROWS_LOAD_WAREHOUSE=? WHERE ID_CONF=? AND FEED_NAME=?";

		PreparedStatement ps = conn.prepareStatement(query);
		ps.setString(1, Action.STAGING_TO_WAREHOUSE.name().toLowerCase());
		if(success) {
			ps.setString(2, Result.SUCCESS.name().toLowerCase());
		}
		else {
			ps.setString(2, Result.FAILED.name().toLowerCase());
		}
		//ps.setString(3, DateTransform.getCurrentDateStr());
		ps.setInt(3, numOfRecords);
		ps.setInt(4, conf.getIdConf());
		ps.setString(5, log.getFeedName());
		
		ps.execute();
		mySQLConn.close(conn);
	}

}
