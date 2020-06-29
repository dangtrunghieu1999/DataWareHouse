package hieu;

import java.sql.SQLException;



public interface Ilog {
	void logLoadToDataWarehouse(Config conf,Log log, int numOfRecords, boolean success) throws SQLException;

}
