package connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

	static Connection con;
	static String WAREHOUSE = "jdbc:mysql://localhost/WareHouse";
	static String CONTROL = "jdbc:mysql://localhost/Control";
	static String STAGING   = "jdbc:mysql://localhost/Staging";
	static String username  = "root";
	static String password  = "trunghieu230899";

	private DBConnection(String dbname) {
		try {
			if (dbname.equals("Control")) {

				con = DriverManager.getConnection(CONTROL, username, password);

			} else if (dbname.equals("WareHouse")) {
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
				if (dbname.equalsIgnoreCase("Control") && CONTROL.equals(url)) {
					return con;
				} else if (dbname.equalsIgnoreCase("WareHouse") && WAREHOUSE.equals(url)) {
					return con;
				} else if (dbname.equalsIgnoreCase("Staging") && STAGING.equals(url)) {
					return con;
				} else {
					new DBConnection(dbname);
					return con;
				}
			}
		
	}
}
