package log;

import java.sql.SQLException;

import control.Configuration;

public interface ILog {
	
	void logDownloadFTPFeed(Configuration conf,Log log, boolean success) throws SQLException;
	void logLoadToStaging(Configuration conf,Log log, int numRecords, boolean success) throws SQLException;
	void logLoadToDataWarehouse(Configuration conf,Log log, int numOfRecords, boolean success) throws SQLException;
}
