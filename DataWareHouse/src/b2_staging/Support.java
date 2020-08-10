package b2_staging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

// ho tro doc file du lieu theo src
public class Support {
	static final String NUMBER_REGEX = "^[0-9]+$";
	static final String ACTIVE_DATE = "31-12-2013";
	static final String DATE_FORMAT = "yyyy-MM-dd";
	public static int row = 0;
	
	// noi file nguon voi ten file de lay ra file du lieu
	
	static public String filePath(String source, String file_name) {
		StringBuffer sourceFile = new StringBuffer(source);
		sourceFile.append("/");
		sourceFile.append(file_name);
		String filePath = sourceFile.toString();
		return filePath;
	}
	
	private static String readLines(String value, String delim) {
		String values = "";
		//khoi tao token de lay value theo delim
		StringTokenizer stoken = new StringTokenizer(value, delim);
	
		int countToken = stoken.countTokens(); // dem count cua column
		String lines = "(";
		for (int j = 0; j < countToken; j++) {
			String token = stoken.nextToken();
			if (Pattern.matches(NUMBER_REGEX, token)) {
				// neu la so thi khong de nhay '
				lines += (j == countToken - 1) ? token.trim() + ")," : token.trim() + ",";
			} else {
				// neu la chu thi de vao dau ' '
				lines += (j == countToken - 1) ? "'" + token.trim() + "')," : "'" + token.trim() + "',";
			}
			values += lines;
			lines = "";
		}
		return values;
	}

	static String readValuesTXT(File s_file, int column) {
		int countRow = 0;
		
		if (!s_file.exists()) {
			return null;
		}
		String values = "";
		String delim  = "|";
		try {
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file), "utf8"));
			String line = bReader.readLine();
			
			// kiem tra vi tri cuoi cung cua read line 
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
			
			// kiem tra readline dau tien co phai la header khong neu khong thi add vao chuoi value
			
			if (Pattern.matches(NUMBER_REGEX, line.split(delim)[0])) {
				values += readLines(line + delim, delim);
				countRow++;
			}
			// read line tung dong trong file 
			
			while ((line = bReader.readLine()) != null) {
				countRow++;
				values += readLines(line + " " + delim, delim);
			}
			
			bReader.close();
			setRow(countRow);
			countRow = 0;
			
			return values.substring(0, values.length() - 1);

		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	static String readFileXLSX(File file, int column) {
		String values = "";
		String value = "";
		String delim = "|";
		int countRow = 0;
		try {
			FileInputStream fileIn = new FileInputStream(file);
			// bo file vao workbook
			
			XSSFWorkbook workBook = new XSSFWorkbook(fileIn);
			// chon mac dinh la sheet 0
			XSSFSheet sheet = workBook.getSheetAt(0);
			
			// lay data row o hang dau tien 
			
			Iterator<Row> rows = sheet.iterator();

			// kiem tra row la header file thi bo qua next row tiep theo
			
			if (rows.next().cellIterator().next().getCellType().equals(CellType.NUMERIC)) {
				rows = sheet.iterator();
			}
			
			// kiem tra row tiep theo co khong
			
			while (rows.hasNext()) {
				countRow++;
				
				Row row = rows.next();
				for (int i = 0; i < column; i++) {
					
					// tu tao du lieu trong khi column trong row trong
					
					Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
					
					// kiem tra cell kieu gi de conver du lieu khong bi sai
					
					CellType cellType = cell.getCellType();
					switch (cellType) {
					case NUMERIC: // numeric kieu du lieu int, double
						if (DateUtil.isCellDateFormatted(cell)) {
							SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
							value += dateFormat.format(cell.getDateCellValue()) + delim;
						} else {
							value += (long) cell.getNumericCellValue() + delim;
						}
						break;
					case STRING:// kieu du lieu chu String
						value += cell.getStringCellValue() + delim;
						break;
					case FORMULA:
						switch (cell.getCachedFormulaResultType()) {
						case NUMERIC:
							value += (long) cell.getNumericCellValue() + delim;
							break;
						case STRING:
							value += cell.getStringCellValue() + delim;
							break;
						default:
							value += " " + delim;
							break;
						}
						break;
					case BLANK:
					default:
						value += " " + delim;
						break;
					}
				}
				if (row.getLastCellNum() == column) {
					value += "|";
				}
				values += readLines(value, delim);
				value = "";
			}
			setRow(countRow);
			countRow = 0;
			workBook.close();
			fileIn.close();
			return values.substring(0, values.length() - 1);
		} catch (Exception e) {
			return null;
		}
	}
	
	public static void setRow(int countRow) {
		row = countRow;
	}
	
	public static int getRow() {
		return row;
	}
	
	
}
