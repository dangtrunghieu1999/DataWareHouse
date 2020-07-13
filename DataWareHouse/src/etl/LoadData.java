package etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

@SuppressWarnings("deprecation")
public class LoadData {
	
	private String NUMBER_REGEX = "1";
	private Connection connection;
	public int countRows = 0;

	public LoadData(Connection connection) {
		this.connection = connection;
	}

	public void loadFromSourceFile() {
		System.out.println("connect success");
		try {
			Statement st = connection.createStatement();
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
			connection = DriverManager.getConnection(Main.JDBC_CONNECTION_URL, Main.username,
					Main.password);
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
	
	private String readLines(String value, String delimited) {
		String values = "";
		StringTokenizer stoken = new StringTokenizer(value, delimited);
		int countToken = stoken.countTokens();
		String lines = "(";
		String token = "";
		for (int j = 0; j < countToken; j++) {
			token = stoken.nextToken();
			lines += (j == countToken - 1) ? '"' + token.trim() + '"' + ")," : '"' + token.trim() + '"' + ",";
			values += lines;
			lines = "";
		}
		return values;
	}
	
	public String readValuesTXT(File s_file, int id_log, int count_field, String delimited) {
		if (!s_file.exists()) {
			return null;
		}
		String values = "";
		
		try {
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file)));
			
			String line = bReader.readLine();
		
			if (new StringTokenizer(line, delimited).countTokens() != count_field) {
				bReader.close();
				return null;
			}
			if (Pattern.matches(NUMBER_REGEX , line.split(delimited)[0])) {
				values += readLines(line, delimited);
			}
			
			while ((line = bReader.readLine()) != null) {

				values += readLines(line + " ", delimited);
				this.countRows++;
			}
			bReader.close();
			return values.substring(0, values.length() - 1);

		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void loadFromCSVOrTXT(int id, String status, String file_name, String source, String des, String user_des,
			String pw_des, String delimited, String field, String table_name, int count_field) {
		
		String filePath = Support.filePath(source, file_name);
		File file = new File(filePath);
		
		String values = readValuesTXT(file, id, count_field,delimited);

		Connection connection = null;
		
		String query = "INSERT INTO " + table_name + " VALUES" + " " + values;
		System.out.println(this.countRows);
		try {
			long start = System.currentTimeMillis();
			connection = DriverManager.getConnection(des, user_des, pw_des);
			connection.setAutoCommit(false);

			PreparedStatement statement = connection.prepareStatement(query);
			statement.execute();
			connection.commit();
			connection.close();
			long end = System.currentTimeMillis();
			System.out.printf("Import done in %d ms\n", (end - start));

			this.countRows = 0;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}