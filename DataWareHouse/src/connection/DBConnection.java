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

	public DBConnection(String dbname) {
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
				} else if (dbname.equalsIgnoreCase("SCP_DOWNLOAD") && STAGING.equals(url)) {
					return con;
				} else {
					new DBConnection(dbname);
					return con;
				}
			}
		
	}
	//
	public static Connection getConnectDB() {
		String nameDB = "control";
		String userName = "root";
		String passwordDB = "";
		try {
			Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			return DriverManager.getConnection("jdbc:mysql://localhost:3306/" + nameDB + "?" + "user=" + userName
					+ "&password=" + passwordDB
					+ "&characterEncoding=UTF-8&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");
		} catch (SQLException ex) {
			// handle any errors
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
			return null;
		}
	}
}
