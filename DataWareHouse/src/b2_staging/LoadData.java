package b2_staging;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import connection.DBConnection;

public class LoadData {

	public void loadFromSourceFile(int id_config) {
		System.out.println("connect success");
		try {
			Connection connectDB = DBConnection.getConnection("CONTROLDB");
			Statement st = connectDB.createStatement();
			
			String query = "select *"
							+ " from logs join config on config.id = logs.id_config"
							+ " where logs.id_config = " + id_config 
							+ " and logs.status = 'ER' limit 1";
			ResultSet rs = st.executeQuery(query);
			
			int id, column_number;
			String status, src_type, source, table_name,file_name;

			while (rs.next()) {
				id        	  = rs.getInt("id");
				file_name 	  = rs.getString("file_name");
				status 	  	  = rs.getString("status");
				src_type  	  = rs.getString("src_type");
				source 	  	  = rs.getString("source");
				table_name	  = rs.getString("table_name");
				column_number = rs.getInt("column_number");
				
				if (src_type.equals("xlsx")) {
					loadFromXLSX(id, status, file_name, source, table_name, column_number, table_name);
				}
				if (src_type.equals("csv") || src_type.equals("txt")) {
					loadFromCSVOrTXT(id, status, file_name, source,table_name, column_number, table_name);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	public void changeStatusFile(String file_name, int numberRows) {
		Date endDate = new Date();
		Connection connection = null;
		try {
			connection = DBConnection.getConnection("CONTROLDB");
			  String query = "update logs set status = ?, time_load_staging = ?, number_row =? where file_name = ?";
		      PreparedStatement preparedStmt = connection.prepareStatement(query);
		      preparedStmt.setString(1,"TR");
		      preparedStmt.setTimestamp(2, new java.sql.Timestamp(endDate.getTime()));
		      preparedStmt.setInt(3, numberRows);
		      preparedStmt.setString(4, file_name);

		      preparedStmt.executeUpdate();
		      System.out.println("success update logs" + file_name);
		      connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Change status error");
		}
	}
	
	public void loadFromXLSX(int id, String status ,String file_name,
							String source, String des, int column_number, String table_name) {
		String filePath = Support.filePath(source, file_name);
		File file 		= new File(filePath);
		String values 	= Support.readFileXLSX(file, column_number);
		String query 	= "Insert into " + table_name + " values" + values;
		System.out.println(query);
		
		Connection connection;
		try {
			long start = System.currentTimeMillis();
			connection = DBConnection.getConnection("Staging");
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(query);
			statement.execute();
			connection.commit();
			
			String queryCount = "Select count(*) from " + table_name;
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery(queryCount);
			rs.next();
		    int count = rs.getInt(1);
		    
			int row = Support.getRow();
			
		    if (count == row) {
		    	changeStatusFile(file_name, count);
				MoveFileStatus.moveFileToSuccess(filePath);
			}
			long end = System.currentTimeMillis();
			System.out.printf("Import done in %d ms\n", (end - start));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadFromCSVOrTXT(int id, String status ,String file_name,
			String source, String des, int column_number, String table_name)  {

		String filePath = Support.filePath(source, file_name);
		File file 		= new File(filePath);
		String values	= Support.readValuesTXT(file, column_number);
		String query 	= "Insert into " + table_name + " values" + values;
		System.out.println(query);
		
		Connection connection;
		
		try {
			connection = DBConnection.getConnection("STAGING");
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(query);
			statement.execute();
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			
		}
	}
	
}