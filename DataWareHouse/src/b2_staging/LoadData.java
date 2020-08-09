package b2_staging;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import common.MailConfig;
import common.SendMail;
import connection.DBConnection;

public class LoadData {

	public void loadFromSourceFile(int id_config) {
		try {
			
			// Ket noi voi DB table control
			
			Connection connectDB = DBConnection.getConnection("Control");
			Statement st = connectDB.createStatement();
			System.out.println("connect success");
			
			String query = "select *"
							+ " from logs join config on config.id = logs.id_config"
							+ " where logs.id_config = " + id_config 
							+ " and logs.status = 'ER' limit 1";
			// execute query
			
			ResultSet rs = st.executeQuery(query);
			
			int id, column_number;
			String status, src_type, source, table_name,file_name;

			if (rs.next()) {
				id        	  = rs.getInt("stt");
				file_name 	  = rs.getString("file_name");
				status 	  	  = rs.getString("status");
				src_type  	  = rs.getString("src_type");
				source 	  	  = rs.getString("source");
				table_name	  = rs.getString("table_name");
				column_number = rs.getInt("column_number");
				
				// load file local from to Staging
				loadToStaging(id_config,id, status, file_name, source, table_name, column_number, table_name,src_type);
			} else {
				System.out.println("result set is Empty");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	// thay doi trang thai file 
	
	public void updateStatusFile(int id,String file_name, int numberRows, int id_config) {
		Date endDate = new Date();
		Connection connection = null;
		try {
			connection = DBConnection.getConnection("Control");
			  String query = "update logs set status = ?, time_load_staging = ?, number_row =? where file_name = ? and stt = ?" ;
		      PreparedStatement preparedStmt = connection.prepareStatement(query);
		      preparedStmt.setString(1,"TR");
		      preparedStmt.setTimestamp(2, new java.sql.Timestamp(endDate.getTime()));
		      preparedStmt.setInt(3, numberRows);
		      preparedStmt.setString(4, file_name);
		      preparedStmt.setInt(5, id);
		      preparedStmt.executeUpdate();
		      System.out.println("success update logs" + file_name);
		      String queryProcess = "update config set config.flag = 'wh' where config.id = " + id_config;
		      PreparedStatement preparedStmtProcess = connection.prepareStatement(queryProcess);
		      preparedStmtProcess.execute();
		      System.out.println("success update config process wh" );
		      
		      connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Change status error");
		}
	}
	
	public void loadToStaging(int id_config,int id, String status ,String file_name,
							String source, String des, int column_number, String table_name, String src_type) {
		// nối thư mục ở local + filename  ra filepath
		String filePath = Support.filePath(source, file_name);
		File file 		= new File(filePath);
		
		// lấy data từ file bỏ vào chuỗi String
		// kiem tra load theo kieu file gi?
		String values = "";
		if (src_type.equalsIgnoreCase("xlsx")) {
			values = Support.readFileXLSX(file, column_number);
		}  else if (src_type.equalsIgnoreCase("txt") || src_type.equalsIgnoreCase("csv")) {
			values = Support.readValuesTXT(file, column_number);
		}
		
		String query 	= "Insert into " + table_name + " values" + values;
		
		// cú pháp query: insert into table_name vales (" "," "," "),(" "," "," ");
		System.out.println(query);
		
		Connection connection;
		try {
			long start = System.currentTimeMillis();
			connection = DBConnection.getConnection("Staging"); // kết nối với DB staging
			connection.setAutoCommit(false);
			PreparedStatement statement = connection.prepareStatement(query);// query 
			statement.execute();
			connection.commit();
			
			// đếm số dòng đã đc load vào db tạm ở Staging 
			String queryCount = "Select count(*) from " + table_name;
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery(queryCount);
			
			rs.next();
		    int count = rs.getInt(1);
		    
			int row = Support.getRow();
			
			if (count == row) {
				updateStatusFile(id, file_name, count, id_config);
				MoveFileStatus.moveFileToSuccess(filePath);
				long end = System.currentTimeMillis();
				SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Successfully!");
				System.out.printf("Import done in %d ms\n", (end - start));
			} 
			else {
				System.out.println("number or row no match");
				MoveFileStatus.moveFileToError(filePath);
				System.out.println("Load file that bai");
				SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Fail!");
			}

		} catch (SQLException e) {
			e.printStackTrace();
			MoveFileStatus.moveFileToError(filePath);
			System.out.println("Load file that bai");
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Fail!");
		}
	}
}