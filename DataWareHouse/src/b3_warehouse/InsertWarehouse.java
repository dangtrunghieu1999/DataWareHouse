package b3_warehouse;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import model.Config;
import model.Log;
import model.Student;
import utils.ConnectDB;
import utils.SystemContain;

public class InsertWarehouse {
	 private static void insert(String sql, Student sv, Connection conn) {
		PreparedStatement pre;
		try {
			pre = conn.prepareStatement(sql);
			pre.setInt(1, sv.getMssv());
			pre.setString(2, sv.getHo());
			pre.setString(3, sv.getTen());
			pre.setDate(4, sv.getDob());
			pre.setString(5, sv.getLop());
			pre.setString(6, sv.getTenlop());
			pre.setString(7, sv.getSdt());
			pre.setString(8, sv.getEmail());
			pre.setString(9, sv.getQuequan());
			pre.setString(10, sv.getGhichu());

			pre.execute();
		} catch (SQLException e) {
		}

	}

	public static void uploadWarehouse(ArrayList<Config> configs, Log log) throws SQLException {
		if (log.getStatus().equals(SystemContain.UPLOAD_STAGING)) {
			Config config_getStaging = null;
			for (Config item : configs) {
				if (item.getNameConfig().equals(SystemContain.CONFIG_GET_STAGING)) {
					config_getStaging = item;
					break;
				}
			}
			PreparedStatement pre = null;
			Connection conn = null;
			ResultSet rs = null;
			conn = ConnectDB.getConnectDB(config_getStaging.getNameDatabase(), config_getStaging.getUserNameDatabase(),
					config_getStaging.getPasswordDatabase());

			pre = conn.prepareStatement(config_getStaging.getQuerySQL());
			pre.setInt(1, log.getId());
			rs = pre.executeQuery();

			ArrayList<Student> students = new ArrayList<Student>();
			while (rs.next()) {
				if (convertToStudent(rs) != null) {
					students.add(convertToStudent(rs));
				}
			}
			conn.close();

			Collections.sort(students, new Comparator<Student>() {

				@Override
				public int compare(Student s1, Student s2) {
					if (s1.getMssv() > s2.getMssv()) {
						return 1;
					} else if (s1.getMssv() < s2.getMssv()) {
						return -1;
					} else {
						return 0;
					}
				}

			});
			Config config_insertWarehouse = null;
			for (Config item : configs) {
				if (item.getNameConfig().equals(SystemContain.CONFIG_UPLOAD_WAREHOUSE)) {
					config_insertWarehouse = item;
					break;
				}
			}
			conn = ConnectDB.getConnectDB(config_insertWarehouse.getNameDatabase(), config_insertWarehouse.getUserNameDatabase(),
					config_insertWarehouse.getPasswordDatabase());
			
			for(Student sv : students) {
				insert(config_insertWarehouse.getQuerySQL(), sv, conn);
			}
			
			conn.close();
			
			Config config_updateLog = null;
			for (Config item : configs) {
				if (item.getNameConfig().equals(SystemContain.CONFIG_UPDATE_LOG)) {
					config_updateLog = item;
					break;
				}
			}
			conn = ConnectDB.getConnectDB(config_updateLog.getNameDatabase(), config_updateLog.getUserNameDatabase(), config_updateLog.getPasswordDatabase());
			log.setStatus(SystemContain.UPLOAD_WAREHOUSE);
			updateLog(config_updateLog.getQuerySQL(), log, conn);
			conn.close();

		}
	}

	public static Student convertToStudent(ResultSet rs) throws SQLException {
		Student student = null;
		try {
			student = new Student();

			student.setIdLog((rs.getInt("id_log")));

			for (int i = 2; i < 12; i++) {
				if (rs.getString(i) == null || rs.getString(i) == "") {
					return null;
				}
			}

			student.setMssv(Integer.parseInt((rs.getString("MSSV"))));
			student.setHo(rs.getString("ho"));
			student.setTen(rs.getString("ten"));
			student.setDob(Date.valueOf(rs.getString("dob")));
			student.setLop(rs.getString("lop"));
			student.setTenlop(rs.getString("tenlop"));
			student.setSdt(rs.getString("sdt"));
			student.setEmail(rs.getString("email"));
			student.setQuequan(rs.getString("quequan"));
			student.setGhichu(rs.getString("ghichu"));
		} catch (Exception e) {
			return null;
		}

		return student;
	}
	
	private static void updateLog(String sql, Log log, Connection conn) {
		PreparedStatement ps;
		try {
			ps = conn.prepareStatement(sql);
			ps.setString(1, log.getStatus());
			ps.setInt(2, log.getId());
			ps.execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getMessage());
		}

	}

}
