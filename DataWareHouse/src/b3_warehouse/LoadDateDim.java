package b3_warehouse;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import connection.DBConnection;
import model.DateDim;

public class LoadDateDim { //Load file datedim.csv vào database warehouse
	public  ArrayList<DateDim> readDateDimFormCSV(String fileName) { // khai báo 1 cái mảng truyền vào filename
		ArrayList<DateDim> dates = new ArrayList<>();
		Path pathToFile = Paths.get(fileName);
		try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII)) {
			String line = br.readLine();
			while (line != null) {
				String[] attributes = line.split(",");
				DateDim date = new DateDim();
				date.setDate_sk(attributes[0]);
				date.setFull_date(attributes[1]);
				date.setDay_since_2005(attributes[2]);
				date.setMonth_since_2005(attributes[3]);
				date.setDay_of_week(attributes[4]);
				date.setCalendar_month(attributes[5]);
				date.setCalendar_year(attributes[6]);
				date.setCalendar_year_month(attributes[7]);
				date.setDay_of_month(attributes[8]);
				date.setDay_of_year(attributes[9]);
				date.setWeek_of_year_sunday(attributes[10]);
				date.setYear_week_sunday(attributes[11]);
				date.setWeek_sunday_start(attributes[12]);
				date.setWeek_of_year_monday(attributes[13]);
				date.setYear_week_monday(attributes[14]);
				date.setWeek_monday_start(attributes[15]);
				date.setHoliday(attributes[16]);
				date.setDay_type(attributes[17]);
				dates.add(date);
				line = br.readLine();
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return dates;
	}

	public void inserDateDim(String nameTableDB, Connection conn, int id, DateDim date) { //insert dữ liệu từ file vào table datedim
		PreparedStatement pre = null;

		String sql = "INSERT INTO warehouse.date_dim (date_sk, full_date, day_since_1980, month_since_1980, day_of_week,"
				+ " calendar_month , calendar_year, calendar_year_month, day_of_month, day_of_year, week_of_year_sunday , year_week_sunday , week_sunday_start ,"
				+ "week_of_year_monday , year_week_monday , week_monday_start , holiday , day_type ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try {
			pre = conn.prepareStatement(sql);

			pre.setInt(1, Integer.parseInt(date.getDate_sk()));
			pre.setString(2, date.getFull_date());
			pre.setString(3, date.getDay_since_2005());
			pre.setString(4, date.getMonth_since_2005());
			pre.setString(5, date.getDay_of_week());
			pre.setString(6, date.getCalendar_month());
			pre.setString(7, date.getCalendar_year());
			pre.setString(8, date.getCalendar_year_month());
			pre.setString(9, date.getDay_of_month());
			pre.setString(10, date.getDay_of_year());
			pre.setString(11, date.getWeek_of_year_sunday());
			pre.setString(12, date.getYear_week_sunday());
			pre.setString(13, date.getWeek_sunday_start());
			pre.setString(14, date.getWeek_of_year_monday());
			pre.setString(15, date.getYear_week_monday());
			pre.setString(16, date.getWeek_monday_start());
			pre.setString(17, date.getHoliday());
			pre.setString(18, date.getDay_type());

			pre.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	public static void main(String[] args) {
		LoadDateDim dd = new LoadDateDim();
		String fileName = "C://Users/rungx/Downloads/Date_Dim/date_dim_without_quarter.csv";
		System.out.println(dd.readDateDimFormCSV(fileName));
		
	}
}
