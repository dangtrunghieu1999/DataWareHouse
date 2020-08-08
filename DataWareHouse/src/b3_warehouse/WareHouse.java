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
			String query = "select table_name from config where id = " + id_config + " and flag = 'wh'";
			System.out.println(query);
			ResultSet rs = st.executeQuery(query);
			rs.next();
			String table_name = rs.getString("table_name");
			
			switch (table_name) {
			case STUDENT:
				addStudentDB();
				break;
			case SUBJECT:
				addSubjectDB();
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	
	
	public void addStudentDB() {
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	public void addSubjectDB() {
		long start = System.currentTimeMillis();
			Connection connectDB;
			try {
				connectDB = DBConnection.getConnection("WareHouse");
				String query = "{CALL addSubject() }";
				PreparedStatement statement = connectDB.prepareStatement(query);
				statement.execute();
				long end = System.currentTimeMillis();
				System.out.printf("Import done load in %d ms\n", (end - start));
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
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	
	
	public static void main(String[] args) {
		WareHouse wh = new WareHouse();
		wh.transformToWareHouse(1);
	}
}
