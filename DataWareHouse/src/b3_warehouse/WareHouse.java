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
			String query = "select table_name from config where id = " + id_config + " and flag = 'wh' "; // lấy
																											// ra
																											// tablename
																											// có
																											// gắn
																											// flag
																											// =
																											// 'wh'
			System.out.println(query);
			ResultSet rs = st.executeQuery(query);// duyệt rs
			if (rs.next()) {
				String table_name = rs.getString("table_name");// nếu đúng
																// thì trả về
																// proceduce
																// tương ứng

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
				addRegister();
					break;

				default:
					break;
				}
			} else {
				System.out.println("result set is Empty");// ngược lại thì...
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void addStudentDB(String tableName) {// phương thức addStudent sẽ
												// tham chiếu tới Stored
												// Procedure
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
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Successfully!");
			updateProcessWh();// create
			updateStatusLog();// create
			System.out.println("Successfull");
		} catch (SQLException e) {
			e.printStackTrace();
			SendMail.sendMail(MailConfig.EMAIL_RECEIVER, MailConfig.EMAIL_TITLE, "Fail!");
		}
	}

	// update process
	public void updateProcessWh() {
		Connection connectDB;
		try {
			connectDB = DBConnection.getConnection("Control");// kết nối đến
																// db control
			String query = "update config set config.flag = 'st' ";// khi config
																	// đã được
																	// load xong
																	// thì cờ
																	// từ 'wh'
																	// đổi
																	// thành
																	// 'st'
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
			connectDB = DBConnection.getConnection("Control");// kết nối đến
																// db control
			String query = "update logs set logs.status = 'Complete' where logs.status = 'TR' ";// thay
																								// đổi
																								// trang
																								// thái
																								// khi
																								// load
																								// xong
																								// từ'TR'
																								// thành'Complete'
			PreparedStatement statement = connectDB.prepareStatement(query);
			statement.execute();

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
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	public static void main(String[] args) {
		WareHouse wh = new WareHouse();
		wh.transformToWareHouse(1);
	}

}