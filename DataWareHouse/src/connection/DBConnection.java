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
	static String password = "42283";

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
		if (con == null || con.isClosed()) {
			new DBConnection(dbname);
			return con;
		} else {
			String url = con.getMetaData().getURL();
			// kiểm tra cái DB muốn lấy và DB hiện tại có giống nhau
			// giống thì trả về con
			// khác thì new mới connection theo dbname
			if (dbname.equalsIgnoreCase("CONTROL") && CONTROLDB.equals(url)) {
				return con;
			} else if (dbname.equalsIgnoreCase("WAREHOUSE") && WAREHOUSE.equals(url)) {
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

	public static Connection getConnectDB(String nameDB, String userName, String passworDB) {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			return DriverManager.getConnection("jdbc:mysql://localhost:3306/" + nameDB + "?" + "user=" + userName
					+ "&password=" + passworDB
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
