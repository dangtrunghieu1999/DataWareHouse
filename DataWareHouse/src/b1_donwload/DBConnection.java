package b1_donwload;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {
	static String USER = "root";
	static String PASS = "";
	static String hostName = "localhost";
	// static String driver = "jdbc:mysql:";
	static String addressConfig = "jdbc:mysql://localhost:3306/control";
	static String source = "jdbc:mysql://localhost:3306/datawarehouse";
	static String dbNameConfig = "myconfig";
	static String dbNameLog = "logs";
	static Connection con;
	static Connection connectionSource;

	public static Connection createConnection() throws SQLException {
		System.out.println("Connecting database....");
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = (Connection) DriverManager.getConnection(addressConfig, USER, PASS);
			System.out.println("Đã kết nối !!!!!");
			System.out.println("----------------------------------------------------------------");
		} catch (ClassNotFoundException e) {
			System.out.println("Không thể kết nối!!!!!!!!!!");
			System.out.println("----------------------------------------------------------------");
		}
		return con;
	}
}
