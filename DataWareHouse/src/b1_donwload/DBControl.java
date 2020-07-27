package b1_donwload;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class DBControl {
	static String tableName;
	String fileType;
	static int numOfcol;
	int numOfdata; // có 20 sv ==> = 20
	static ArrayList<String> columnsList = new ArrayList<String>();
	int numberColumns;
	static String dataPath;
	static String delimeter;
	static String source;
	// lấy cofig để tải file trên nó
	static String host;
	static int port;
	static String username;
	static String password;
	static String remotePath;
	static String localPath;
	static String dirLog;
	static String download;

	public static void getConfig(int id) throws SQLException {
		PreparedStatement pre = (PreparedStatement) DBConnection.createConnection()
				.prepareStatement("SELECT * FROM control.myconfig where id=?;");
		pre.setInt(1, id);
		ResultSet tmp = pre.executeQuery();
		tmp.next();
		tableName = tmp.getString("nameTable");
		numOfcol = Integer.parseInt(tmp.getString("numOfCol"));
		String listofcol = tmp.getString("listField");

		dataPath = tmp.getString("dataPath");
		// URL db đưa lên: jdbc:mysql://localhost:3306/Datawarehouse
		delimeter = tmp.getString("delimiter");

		StringTokenizer tokens = new StringTokenizer(listofcol, delimeter);
		System.out.println("......Source URL.");
		source = tmp.getString("SourceURL");
		// using step1 down file
		host = tmp.getString("hostAcess");
		username = tmp.getString("userNameAcess");
		password = tmp.getString("passAcess");
		remotePath = tmp.getString("remotePathAcess");
		localPath = tmp.getString("localPathAcesss");
		// dirLog
		dirLog = tmp.getString("dirLog");

		while (tokens.hasMoreTokens()) {
			columnsList.add(tokens.nextToken());
		}
		System.out.println("Get config: complete!!!!");
	}
}
