package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

	static Connection con;
	static String WAREHOUSE = "jdbc:mysql://localhost:3306/Datawarehouse";
	static String CONTROLDB = "jdbc:mysql://localhost/Control";
	static String STAGING = "jdbc:mysql://localhost/Staging";
	static String username = "root";
	static String password = "";

	private DBConnection(String dbname) {
		try {
			if (dbname.equals("CONTROLDB")) {

				con = DriverManager.getConnection(CONTROLDB, username, password);

			} else if (dbname.equals("WAREHOUSE")) {
				con = DriverManager.getConnection(WAREHOUSE, username, password);
			} else {
				con = DriverManager.getConnection(STAGING, username, password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection(String dbname) throws SQLException {
			// kiểm tra connection bằng null hay bị đóng kết nối thì tạo lại
			if (con == null||con.isClosed()) {
				new DBConnection(dbname);
				return con;
			} else {
				 String url = con.getMetaData().getURL();
				 // kiểm tra cái DB muốn lấy và DB hiện tại có giống nhau
				 //giống thì trả về con
				 //khác thì new mới connection theo dbname
				if (dbname.equalsIgnoreCase("CONTROL") && CONTROLDB.equals(url)) {
					return con;
				} else if (dbname.equalsIgnoreCase("WAREHOUSE") && WAREHOUSE.equals(url)) {
					return con;
				} else if (dbname.equalsIgnoreCase("STAGING") && STAGING.equals(url)) {
					return con;
				} else {
					new DBConnection(dbname);
					return con;
				}
			}
		
	}
}
