package log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import connections.MySQLConnection;
import constants.Action;
import constants.Constant;
import constants.Result;
import control.Configuration;
import transform.DateDim;

public class MySQLLog extends Constant implements ILog {
	

	@Override
	public void logDownloadFTPFeed(Configuration conf,Log log, boolean success) throws SQLException {
	}

	@Override
	public void logLoadToStaging(Configuration conf,Log log, int numRecords, boolean success) throws SQLException {
	}

	@Override
	public void logLoadToDataWarehouse(Configuration conf,Log log, int numOfRecords, boolean success) throws SQLException {


	}
}
