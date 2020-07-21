package b2_staging;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import connection.DBConnection;

@SuppressWarnings("deprecation")
public class LoadData {
	
	public int countRows = 0;
	public void loadFromSourceFile() {
		System.out.println("connect success");
		try {
			Connection connectDB = DBConnection.getConnection("CONTROLDB");
			Statement st = connectDB.createStatement();
			ResultSet rs = st.executeQuery("select * from logs join config on config.id = logs.id");
			String file_name, status, src_type, delimited,source, des,user_des,pw_des,field,table_name;
			int id, count_field;

			while (rs.next()) {
				id        	= rs.getInt("id");
				file_name 	= rs.getString("file_name");
				status 	  	= rs.getString("status");
				src_type  	= rs.getString("src_type");
				delimited 	= rs.getString("delimited");
				source 	  	= rs.getString("source");
				des 		= rs.getString("destination");
				user_des	= rs.getString("user_des");
				pw_des  	= rs.getString("pw_des");
				field	    = rs.getString("field");
				table_name	= rs.getString("table_name");
				count_field = rs.getInt("count_field");
				
				if (src_type.equals("xlsx")) {
					loadFromXLSX(id, status, file_name, source, des,
							user_des, pw_des, delimited, field, table_name);
				}
				if (src_type.equals("csv") || src_type.equals("txt")) {
					loadFromCSVOrTXT(id, status, file_name, source, des, 
							user_des, pw_des, delimited, field,table_name,count_field);
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
							String source, String des, String user_des,
							String pw_des, String delimited, String field, String table_name) {
		String filePath = Support.filePath(source, file_name);
		Connection connection = null;
		int batchSize = 20;
		int countRows = 0;
		int checkRows = 0;
		
		try {
			connection = DriverManager.getConnection(des, user_des, pw_des);
			connection.setAutoCommit(false);
			long start = System.currentTimeMillis();

			FileInputStream inputStream = new FileInputStream(filePath);
			Workbook workbook = new XSSFWorkbook(inputStream);

			Sheet firstSheet = workbook.getSheetAt(0);
			Iterator<Row> rowIterator = firstSheet.iterator();

			String query = Support.convertQuery(field, table_name);
			
			PreparedStatement statement = connection.prepareStatement(query);
			
			int count = 0;
			int counterSecond = 2;
			rowIterator.next();
			
			while (rowIterator.hasNext()) {
				Row nextRow = rowIterator.next();
				Iterator<Cell> cellIterator = nextRow.cellIterator();
				checkRows++;
				while (cellIterator.hasNext()) {
					
					Cell nextCell = cellIterator.next();
					int columnIndex = nextCell.getColumnIndex();
					
					switch (columnIndex) {
					case 0:
						int stt = (int) nextCell.getNumericCellValue();
						statement.setInt(1, stt);
						break;
					default:
						switch (nextCell.getCellType()) {
						case NUMERIC:
							if (HSSFDateUtil.isCellDateFormatted(nextCell)) {
								Date valueDate = nextCell.getDateCellValue();
								statement.setTimestamp(counterSecond, new Timestamp(valueDate.getTime()));
								break;
							} else {
								Double valueDouble = (Double) nextCell.getNumericCellValue();
								statement.setDouble(counterSecond, valueDouble);
							}
							break;
						case STRING:
							String valueString = nextCell.getStringCellValue();
							statement.setString(counterSecond, valueString);
							break;
						case BLANK:
							statement.setString(counterSecond, "null");
							break;
						default:
							break;
						}
						counterSecond++;
						break;
					}
				}
				counterSecond = 2;
				statement.addBatch();
				
				if (count % batchSize == 0) {
					statement.executeBatch();
					countRows++;
				}
			}
			workbook.close();
			connection.commit();
			
			if (countRows == checkRows) {
				long end = System.currentTimeMillis();
				System.out.printf("Import done in %d ms\n", (end - start));
				changeStatusFile(file_name, countRows);
				MoveFileStatus.moveFileToSuccess(filePath);
			} else {
				System.out.println(" loadToStaging that bai ");
				MoveFileStatus.moveFileToError(filePath);
			}
		} catch (Exception e) {
			System.out.println(" loadToStaging that bai ");
			MoveFileStatus.moveFileToError(filePath);
		}
		
	}
	
	public void loadFromCSVOrTXT(int id, String status, String file_name, String source, String des, String user_des,
			String pw_des, String delimited, String field, String table_name, int count_field) throws SQLException {

		String filePath = Support.filePath(source, file_name);
		File file = new File(filePath);
		Connection connection = null;

		String loadQuery = "LOAD DATA INFILE '" + file + "' INTO TABLE data FIELDS TERMINATED BY '\\" + delimited
				+ "' LINES TERMINATED BY '\n' IGNORE " + 1 + " LINES";
		System.out.println(loadQuery);
		PreparedStatement state;
		try {
			connection = DriverManager.getConnection(des, user_des, pw_des);
			connection.setAutoCommit(false);
			state = connection.prepareStatement(loadQuery);
			state.executeUpdate();
			state.close();
			connection.commit();
			
		} catch (SQLException e) {
			e.printStackTrace();
			connection.rollback();
		}

	}
	
}