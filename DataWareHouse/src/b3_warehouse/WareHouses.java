package step3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.mysql.jdbc.CallableStatement;

import connection.DBConnection;
import model.Log;
import model.Student;

public class WareHouses {
	public static final String STUDENT = "Student";
	public static final String SUBJECT = "Subject";
	public static final String DANGKY  = "DangKy";
	
	
	public void transformToWareHouse(int id_config) {
		// khoi tao ket noi  
		Connection connectDB;
		try {
			// Ket noi toi DB Control
			connectDB = DBConnection.getConnection("CONTROLDB");
			Statement st = connectDB.createStatement();
			// Tao cau query
			String query = "select table_name from config where id = " + id_config ;
			System.out.println(query);
			//Thuc thi cau lenh query
			ResultSet rs = st.executeQuery(query);
			rs.next();
			String table_name = rs.getString("table_name");
			
			switch (table_name) {
			case STUDENT:
				getListDataStudent();
				break;
			case SUBJECT:
				System.out.println("b");
				break;
				
			case DANGKY:
				System.out.println("c");
				break;
				
			default:
				break;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public void getListDataStudent() {
		Connection connectDB;
		long start = System.currentTimeMillis();
		try {
			// Ket noi toi DB Staging
			connectDB = DBConnection.getConnection("Staging");
			Statement st = connectDB.createStatement();
			// Tao cau query
			String query = "select * from Student";
			System.out.println(query);
			//Thuc thi cau lenh query
			ResultSet rs = st.executeQuery(query);
			ArrayList<Student> listStudents = new ArrayList<Student>();
			String mssv,holot,ten,ngaysinh,lop,tenlop,sdt,email,quequan,ghichu;
			//Duyệt ResultSet rs
			while(rs.next()) {
				 mssv  		= rs.getString("f2");
				 holot 		= rs.getString("f3");
				 ten	 	= rs.getString("f4");
				 ngaysinh 	= rs.getString("f5");
				 lop		= rs.getString("f6");
				 tenlop  	= rs.getString("f7");
				 sdt 		= rs.getString("f8");
				 email    	= rs.getString("f9");
				 quequan  	= rs.getString("f10");
				 ghichu   	= rs.getString("f11");
				
				 Student s = new Student();
				 s.setStudentId(mssv);
				 s.setFirstName(holot);
				 s.setLastName(ten);
				 s.setBirthDay(ngaysinh);
				 s.setClassId(lop);
				 s.setClassName(tenlop);
				 s.setPhoneNumber(sdt);
				 s.setEmail(email);
				 s.setHomeTown(quequan);
				 s.setNotes(ghichu);
				 listStudents.add(s);
			}
			//Xuat ra danh sach sv
			System.out.println(listStudents.size());
			long end = System.currentTimeMillis();
			//add vao wh
			addStudentDB(listStudents);
			System.out.printf("Import done in %d ms\n", (end - start));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void addStudentDB(ArrayList<Student> list) {
		long start = System.currentTimeMillis();
		int size = list.size();
			Connection connectDB;
			try {
				// Ket noi toi DB WH
				connectDB = DBConnection.getConnection("WareHouse");
				//Tao cau query
				String query = "{CALL WareHouse.inser_student(?,?,?,?,?,?,?,?,?,?)}";
				// thuc thi Stored Procedure, co the tra ve nhieu doi tuong
				CallableStatement stmt = (CallableStatement) connectDB.prepareCall(query);
				for (int i = 0; i < size; i++) {
				stmt.setString(1, list.get(i).getStudentId());
				stmt.setString(2, list.get(i).getFirstName());
				stmt.setString(3, list.get(i).getLastName());
				stmt.setString(4, list.get(i).getBirthDay());
				stmt.setString(5, list.get(i).getClassId());
				stmt.setString(6, list.get(i).getClassName());
				stmt.setString(7, list.get(i).getPhoneNumber());
				stmt.setString(8, list.get(i).getEmail());
				stmt.setString(9, list.get(i).getHomeTown());
				stmt.setString(10, list.get(i).getNotes());
				stmt.executeQuery();
				}
				long end = System.currentTimeMillis();
				System.out.printf("Import done load in %d ms\n", (end - start));
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	//Cap nhat trang thai log
	public static void updateLog(String sql, Log log, Connection conn) {
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, log.getStatus());
			ps.setInt(2, log.getId());
			ps.execute();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}

	}
	
	public static void main(String[] args) {
		WareHouses wh = new WareHouses();
		wh.transformToWareHouse(1);
	}
}
