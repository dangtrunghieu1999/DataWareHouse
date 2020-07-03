package etl;

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
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class LoadData {
	
	private static final String SQL_INSERT = "INSERT INTO ${table} VALUES (${values})";
	private static final String TABLE_REGEX = "\\$\\{table\\}";
	private static final String VALUES_REGEX = "\\$\\{values\\}";
	private Connection connection;

	public LoadData(Connection connection) {
		this.connection = connection;
	}

	public void loadFromSourceFile() {
		System.out.println("connect success");
		try {
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery("select * from logs join config on config.id = logs.id");
			String file_name, status, src_type, delimited,source, des,user_des,pw_des,field,table_name;
			int id, ignore;

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
				ignore 		= rs.getInt("ignore");
				
				if (src_type.equals("xlsx")) {
					loadFromXLSX(id, status, file_name, source, des,
							user_des, pw_des, delimited, field, table_name);
				}
				if (src_type.equals("csv") || src_type.equals("txt")) {
					loadFromCSVOrTXT(id, status, file_name, source, des, 
							user_des, pw_des, delimited, field,table_name,ignore);
				}
			}
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	public String filePath(String source, String file_name) {
		StringBuffer sourceFile = new StringBuffer(source);
		sourceFile.append("/");
		sourceFile.append(file_name);
		String filePath = sourceFile.toString();
		return filePath;
		
	}
	
	public String convertQuery(String field) {
		String [] headerRow = field.split(",");
		
		String questionmarks = StringUtils.repeat("?,", headerRow.length);
		
		questionmarks = (String) questionmarks.subSequence(0, questionmarks
				.length() - 1);
		String query = SQL_INSERT.replaceFirst(TABLE_REGEX, "Student");
		
		query = query.replaceFirst(VALUES_REGEX, questionmarks);
		return query;
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
		      preparedStmt.setTimestamp(2, new java.sql.Timestamp(endDate.getTime()));
		      preparedStmt.setString(3, file_name);

		      preparedStmt.executeUpdate();
		      System.out.println("success update logs" + file_name);
		      connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void loadFromXLSX(int id, String status ,String file_name,
							String source, String des, String user_des,
							String pw_des, String delimited, String field, String table_name) {
		String filePath = filePath(source, file_name);
		
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

			String query = convertQuery(field);
			System.out.println(query);
			PreparedStatement statement = connection.prepareStatement(query);
			
			int count = 0;
			int counterSecond = 2;
			rowIterator.next();
			

			while (rowIterator.hasNext()) {
				Row nextRow = rowIterator.next();
				Iterator<Cell> cellIterator = nextRow.cellIterator();

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
							Double valueDouble = (Double) nextCell.getNumericCellValue();
							statement.setDouble(counterSecond, valueDouble);
							break;
						case STRING:
							String valueString = nextCell.getStringCellValue();
							statement.setString(counterSecond, valueString);
							break;
						case BLANK:
							statement.setString(counterSecond, "null");
							break;
						case _NONE:
							Date valueDate = nextCell.getDateCellValue();
							statement.setTimestamp(counterSecond, new Timestamp(valueDate.getTime()));
							break;
						default:
							break;
						}
						
						counterSecond ++;
						break;
					}
				}
				counterSecond = 2;

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
	
	public void loadFromCSVOrTXT(int id, String status ,String file_name,
								String source, String des, String user_des,
								String pw_des, String delimited,
								String field,String table_name,int ignore) {

		String filePath = filePath(source, file_name);
		
		Connection connection = null;

		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader bReader = null;
		
		int i = 0;
		
		String query = convertQuery(field);
		
		System.out.println(query);
		try {
			
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager.getConnection(des, user_des, pw_des);
			connection.setAutoCommit(false);
			
			fis = new FileInputStream(filePath);
			isr = new InputStreamReader(fis);
			bReader = new BufferedReader(isr);
			
			String lineText = null;
			StringTokenizer st;
			
			PreparedStatement statement = connection.prepareStatement(query);
			
			if (ignore == 0) {
				
				while ((lineText = bReader.readLine()) != null) {
					st = new StringTokenizer(lineText, delimited);
					i = 0;
					while (st.hasMoreElements()) {
						statement.setString(++i, st.nextToken());
					}
					statement.execute();
				}
				
			}else {
				
				lineText = bReader.readLine();
				st = new StringTokenizer(lineText, delimited);
				
				while ((lineText = bReader.readLine()) != null) {
					st = new StringTokenizer(lineText, delimited);
					i = 0;
					while (st.hasMoreElements()) {
						statement.setString(++i, st.nextToken());
					}
					statement.execute();
				}
			}
			


			
			System.out.println("Insert success record");
			updateLogs(file_name);
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