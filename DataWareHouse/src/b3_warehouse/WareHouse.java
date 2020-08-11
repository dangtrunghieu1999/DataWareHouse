package b3_warehouse;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import common.MailConfig;
import common.SendMail;
import connection.DBConnection;

public class WareHouse {
	public static final String STUDENT = "Student";
	public static final String SUBJECT = "Subject";
	public static final String CLASS = "Class";
	public static final String REGISTER = "Register";

	public void transformToWareHouse(int id_config) {
		Connection connectDB;// khởi tạo kết nối
		try {
			connectDB = DBConnection.getConnection("Control");// kết nối đến
																// db control
			Statement st = connectDB.createStatement();// tạo statement
			//lấy ra table name có gắn cờ flag= 'wh'
			String query = "select table_name from config where id = " + id_config + " and flag = 'wh' "; // 
			System.out.println(query);
			ResultSet rs = st.executeQuery(query);
			// duyệt rs
			if (rs.next()) { 
				//nếu đúng thì trả về procedure tương ứng
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
			} else {
				//ngược lại thì thì send mail báo lỗi
				System.out.println("result set is Empty");
				SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Result set is Empty!");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
// phương thức addStudent nhận vào tableName tham chiếu tới procedure tương tứng trong Stored Procedure
	public void addStudentDB(String tableName) {
		long start = System.currentTimeMillis(); // time start trong hệ thống
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("WareHouse");// kết nối
																// đến db
																// warehoue
			String query = "{CALL addStudent() }";// gọi đến procedure
													// allStudent trong mysql
			PreparedStatement statement = connectDB.prepareStatement(query);
			statement.execute();
			long end = System.currentTimeMillis();// time end trong hệ thống
			System.out.printf("Import done load in %d ms\n", (end - start));
			//cập nhật flag thành 'st'
			updateProcessWh();
			//thay đổi status trong log thành 'Complete'
			updateStatusLog();
			//xóa dữ liệu ở table staging
			truncateTableStaging(tableName);
			System.out.println("Successfull");
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Successfull");
		} catch (SQLException e) {
			e.printStackTrace();
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Fail!");
		}
	}

	// update process
	public void updateProcessWh() {
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("Control");// kết nối đến db control
			// khi load xong dữ liệu thì flag sẽ thay đổi 'wh' đổi thành 'st'
			String query = "update config set config.flag = 'st' ";
																	
			PreparedStatement statement = connectDB.prepareStatement(query);
			statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// thay đổi trạng thái log
	public void updateStatusLog() {
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("Control");// kết nối đến db control
			//thay đổi trang thái khi load dữ liệu xong từ'TR' thành'Complete'												
			String query = "update logs set logs.status = 'Complete' where logs.status = 'TR' ";
			PreparedStatement statement = connectDB.prepareStatement(query);
			statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// xóa dữ liệu cũ ở table staging
	private void truncateTableStaging(String table) {
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
	// phương thức addSubjectnhận vào tableName tham chiếu tới procedure tương tứng trong Stored Procedure
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
			truncateTableStaging(tableName);
			System.out.println("Successfull");
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Successfull");
		} catch (SQLException e) {
			e.printStackTrace();
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Fail!");
		}
	}

	// phương thức addClassDB nhận vào tableName tham chiếu tới procedure tương tứng trong Stored Procedure
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
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Successfull");
		} catch (SQLException e) {
			e.printStackTrace();
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Fail!");
		}
	}
	// phương thức addRegister nhận vào tableName tham chiếu tới procedure tương tứng trong Stored Procedure
	public void addRegister() {
		long start = System.currentTimeMillis();
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("WareHouse");
			String query = "{CALL addRegister()}";
			PreparedStatement statement = connectDB.prepareStatement(query);
			statement.execute();
			long end = System.currentTimeMillis();
			System.out.printf("Import done load in %d ms\n", (end - start));
			updateProcessWh();
			updateStatusLog();
			statement.close();
			connectDB.close();
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Successfull");
		} catch (SQLException e) {
			e.printStackTrace();
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Fail!");
		}
	}

	public static void main(String[] args) {
		WareHouse wh = new WareHouse();
		wh.transformToWareHouse(1);
	}

}