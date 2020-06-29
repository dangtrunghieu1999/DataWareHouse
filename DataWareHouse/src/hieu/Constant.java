package hieu;

public abstract class Constant {
	
	public static final String DRIVER = "com.mysql.cj.jdbc.Driver";
	public static final String USER = "root";
	public static final String PASS = "42283";
	
	public static final String URL_CONTROL = "jdbc:mysql://127.0.0.1:3306/CONTROL_DATABASE?useSSL=false";
	public static final String URL_STAGING = "jdbc:mysql://127.0.0.1:3306/STAGING?useSSL=false";
	public static final String URL_WAREHOUSE = "jdbc:mysql://127.0.0.1:3306/WAREHOUSE?useSSL=false";

}
