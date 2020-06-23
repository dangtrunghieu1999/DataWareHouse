package data_warehouse;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class LoadData {
	
	private static final String SQL_INSERT = "INSERT INTO ${table} (${keys}) VALUES (${values})";
	private static final String TABLE_REGEX = "\\$\\{table\\}";
	private static final String KEYS_REGEX = "\\$\\{keys\\}";
	private static final String VALUES_REGEX = "\\$\\{values\\}";
	int counter = 1;
	private Connection connection;

	public LoadData(Connection connection) {
		this.connection = connection;
	}

	public void loadFromSourceFile() {
		System.out.println("connect success");
		try {
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery("select * from logs join config on config.id = logs.id");
			String file_name, status, src_type, delimited,source, des,user_des,pw_des,field;
			int id;

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
				
				if (src_type.equals("xlsx")) {
					loadFromXLSX(id, status, file_name, source, des, user_des, pw_des, delimited, field);
				}
			
				
			}
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	public void loadFromXLSX(int id, String status ,String file_name,String source, String des, String user_des,
			String pw_des, String delimited, String field) {
		StringBuffer file = new StringBuffer(source);
		file.append("/");
		file.append(file_name);
		String filePath = file.toString();
		
		Connection connection = null;
		int batchSize = 20;
		
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(des, user_des, pw_des);
			connection.setAutoCommit(false);
			
			
			long start = System.currentTimeMillis();

			FileInputStream inputStream = new FileInputStream(filePath);
			Workbook workbook = new XSSFWorkbook(inputStream);

			Sheet firstSheet = workbook.getSheetAt(0);
			Iterator<Row> rowIterator = firstSheet.iterator();

			
			String [] headerRow = field.split(",");
			String questionmarks = StringUtils.repeat("?,", headerRow.length);
			questionmarks = (String) questionmarks.subSequence(0, questionmarks
					.length() - 1);
			String query = SQL_INSERT.replaceFirst(TABLE_REGEX, "Student");
			query = query
					.replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, ","));
			query = query.replaceFirst(VALUES_REGEX, questionmarks);
			
			System.out.println(query);
			PreparedStatement statement = connection.prepareStatement(query);
			
			int count = 0;
			rowIterator.next();
			

			while (rowIterator.hasNext()) {
				Row nextRow = rowIterator.next();
				Iterator<Cell> cellIterator = nextRow.cellIterator();

				while (cellIterator.hasNext()) {
					Cell nextCell = cellIterator.next();
					
					int columnIndex = nextCell.getColumnIndex();

					switch (columnIndex) {
					case 0:
						statement.setInt(1, counter);
						counter++;
						break;
					case 1:
						switch (nextCell.getCellType()) {
						case NUMERIC:
							Double masv = (Double) nextCell.getNumericCellValue();
							statement.setDouble(2, masv);
							break;
						case STRING:
							String mssvString = nextCell.getStringCellValue();
							statement.setString(2, mssvString);
							break;
						default:
							break;
						}

						break;
					case 2:
						String first_name = nextCell.getStringCellValue();
						statement.setString(3, first_name);
						break;
					case 3:
						String last_name = nextCell.getStringCellValue();
						statement.setString(4, last_name);
						break;
					case 4:
						switch (nextCell.getCellType()) {
						case STRING: 
							String birthdayString = (String) nextCell.getStringCellValue();
							statement.setString(5, birthdayString);
							break;
						default:
							Date birthday = nextCell.getDateCellValue();
							statement.setTimestamp(5, new Timestamp(birthday.getTime()));
						}
						break;
					case 5:
						String id_class = nextCell.getStringCellValue();
						statement.setString(6, id_class);
						break;
					case 6:
						String name_class = nextCell.getStringCellValue();
						statement.setString(7, name_class);
						break;
					case 7:
						switch (nextCell.getCellType()) {
						case NUMERIC: 
							Double phone = nextCell.getNumericCellValue();
							statement.setDouble(8, phone);
							break;
							
						case STRING:
							String phoneString = nextCell.getStringCellValue();
							statement.setString(8, phoneString);
							break;
							
						default:
							break;
						}
					
						break;
					case 8:
						String email = nextCell.getStringCellValue();
						statement.setString(9, email);
						break;
					case 9:
						String home_town = nextCell.getStringCellValue();
						statement.setString(10, home_town);
						break;
					case 10:
						String notes = nextCell.getStringCellValue();
						statement.setString(11, notes);
						break;
					}
				}

				statement.addBatch();

				if (count % batchSize == 0) {
					statement.executeBatch();
				}
			}

			workbook.close();

			// execute the remaining queries
			statement.executeBatch();

			connection.commit();

			long end = System.currentTimeMillis();
			System.out.printf("Import done in %d ms\n", (end - start));
			
			updateLogs(file_name);
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
	}
	
	public void updateLogs(String file_name) {
		Date endDate = new Date();
		
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(Main.JDBC_CONNECTION_URL, Main.username,
					Main.password);
			  String query = "update logs set status = ?, time_upload = ? where file_name = ?";
		      PreparedStatement preparedStmt = connection.prepareStatement(query);
		      preparedStmt.setString(1,"TER");
//		      preparedStmt.setDate(2, java.sql.Date.valueOf(java.time.LocalDate.now()));
		      preparedStmt.setTimestamp(2, new java.sql.Timestamp(endDate.getTime()));
		      preparedStmt.setString(3, file_name);

		      preparedStmt.executeUpdate();
		      System.out.println("success" + file_name);
		      connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadFromCSVOrTXT(String source_file, String des, String user_des,
			String pw_des, String delimited, String field) {
		
		Connection connection = null;

		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader bReader = null;
		
		int START_LINE = 1;
		int counter = START_LINE;
		
		String [] headerRow = field.split(",");
		String questionmarks = StringUtils.repeat("?,", headerRow.length);
		questionmarks = (String) questionmarks.subSequence(0, questionmarks
				.length() - 1);
		String query = SQL_INSERT.replaceFirst(TABLE_REGEX, "Student");
		query = query
				.replaceFirst(KEYS_REGEX, StringUtils.join(headerRow, ","));
		query = query.replaceFirst(VALUES_REGEX, questionmarks);
		
		System.out.println(query);
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(des, user_des, pw_des);
			connection.setAutoCommit(false);
			
			fis = new FileInputStream(source_file);
			isr = new InputStreamReader(fis);
			bReader = new BufferedReader(isr);
			
			PreparedStatement statement = connection.prepareStatement(query);
			
			String line = null;
			String arrayData[] = null;
			
			while (true) {
				line = bReader.readLine();
				if (line == null) {
					break;
				} else {
					if (counter > START_LINE) {
						arrayData = line.split(delimited);
						statement.setInt(1, this.counter);
						for (int i = 1; i < arrayData.length; i++) {
							statement.setString(i+1, arrayData[i]);
						}
						statement.executeUpdate();
					}
					counter++;
					this.counter++;

				}
				connection.commit();

			}
			System.out.println("Insert success record");
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

			try {
				bReader.close();
				isr.close();
				fis.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
		
	}
	
	
}
