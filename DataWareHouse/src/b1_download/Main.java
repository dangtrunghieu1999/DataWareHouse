package b1_download;

import java.sql.SQLException;

import connection.DBConnection;
import model.Config;

public class Main {
	public static void main(String[] args) throws SQLException {
		System.out.println("Thành công");
		Config cf = new Config(1, "config", "drive.ecepvn.org", 2227, "root", "hoangton03", ",",
				"/volume1/ECEP/song.nguyen/DW_2020/data", "F:\\HK6-2020\\DataWareHouse_ThaySong\\SCP_DownLoad",
				"control", "config", "log", "staging", "warehouse");

		DownloadAndInsertLog.downloadAndInsertLog(cf, DBConnection.getConnection("CONTROLDB"));

	}
}
