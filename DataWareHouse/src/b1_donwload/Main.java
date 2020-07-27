package b1_donwload;

import java.sql.SQLException;

public class Main {
	public static void main(String[] args) throws SQLException {
		DBControl db = new DBControl();
		DBControl.getConfig(1);
		FileDownLoad download = new FileDownLoad();
		System.out.println(download.downloadFile());
		System.out.println("DownLoad thành công!");

	}
}
