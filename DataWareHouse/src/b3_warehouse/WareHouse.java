package b3_warehouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


import connection.DBConnection;

public class WareHouse {
	public static final String STUDENT   = "Student";
	public static final String SUBJECT   = "Subject";
	public static final String CLASS   	 = "Class";
	public static final String REGISTER  = "Register";
	
	public void transformToWareHouse(int id_config) {
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("Control");
			Statement st = connectDB.createStatement();
			String query = "select table_name from config where id = " + id_config
					+ " and flag = 'wh' ";
			System.out.println(query);
			ResultSet rs = st.executeQuery(query);
			if (rs.next()) {
				String table_name = rs.getString("table_name");
				
				switch (table_name) {
				case STUDENT:
					addStudentDB(table_name);
					break;
				case SUBJECT:
					addSubjectDB(table_name);
					break;
				case CLASS:
					addClassDB();
					break;
					
				case REGISTER:
					System.out.println("c");
					break;
					
				default:
					break;
				}
			}else {
				System.out.println("result set is Empty");
			}
			st.close();
			connectDB.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	
	
	public void addStudentDB(String tableName) {
		long start = System.currentTimeMillis();
			Connection connectDB;
			try {
				connectDB = DBConnection.getConnection("WareHouse");
				String query = "{CALL addStudent() }";
				PreparedStatement statement = connectDB.prepareStatement(query);
				statement.execute();
				long end = System.currentTimeMillis();
				System.out.printf("Import done load in %d ms\n", (end - start));
				updateProcessWh();
				updateStatusLog();
				trucateTableStaging(tableName);
				System.out.println("Successfull");
				statement.close();
				connectDB.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	public void updateProcessWh() {
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("Control");
			String query = "update config set config.flag = 'st' ";
			PreparedStatement statement = connectDB.prepareStatement(query);
			statement.execute();
			statement.close();
			connectDB.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void updateStatusLog() {
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("Control");
			String query = "update logs set logs.status = 'Complete' where logs.status = 'TR' ";
			PreparedStatement statement = connectDB.prepareStatement(query);
			statement.execute();
			statement.close();
			connectDB.close();		
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void trucateTableStaging(String table) {
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("Staging");
			String query = "TRUNCATE " + table;
			PreparedStatement statement = connectDB.prepareStatement(query);
			statement.execute();
			statement.close();
			connectDB.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void addSubjectDB(String tableName) {
		long start = System.currentTimeMillis();
			Connection connectDB;
			try {
				connectDB = DBConnection.getConnection("WareHouse");
				String query = "{CALL addSubject() }";
				PreparedStatement statement = connectDB.prepareStatement(query);
				statement.execute();
				long end = System.currentTimeMillis();
				System.out.printf("Import done load in %d ms\n", (end - start));
				updateProcessWh();
				updateStatusLog();
				trucateTableStaging(tableName);
				statement.close();
				connectDB.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	public void addClassDB() {
		long start = System.currentTimeMillis();
			Connection connectDB;
			try {
				connectDB = DBConnection.getConnection("WareHouse");
				String query = "{CALL addClass()}";
				PreparedStatement statement = connectDB.prepareStatement(query);
				statement.execute();
				long end = System.currentTimeMillis();
				System.out.printf("Import done load in %d ms\n", (end - start));
				statement.close();
				connectDB.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	public static void main(String[] args) {
		WareHouse wh = new WareHouse();
		wh.transformToWareHouse(1);
	}
	
}
