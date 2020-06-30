package etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {
	
	public static String JDBC_CONNECTION_URL = 
			"jdbc:mysql://localhost:3306/Control";
	public static String username = "root";
	public static String password = "trunghieu230899";
	
	public static void main(String[] args) {
		LoadData load = new LoadData(getConnection());
		load.loadFromSourceFile();
		
	}
	
	
	private static Connection getConnection() {
		Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(JDBC_CONNECTION_URL, username, password);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return connection;
	}
}
