package model;

public class Config {
	private int id_Config;
	private String name_Config;
	private String hostName;
	private int port;
	private String userNameAccount;
	private String passwordAccount;
	private String file_Format;
	private String remote_Dir;
	private String local_Dir;
	
	
	private String name_db_Control;
	private String name_table_config;
	private String name_table_log;
	
	private String name_db_Staging;
	private String name_table_staging;
	
	private String name_db_Warehouse;
	private String name_table_warehouse;
	
	
	public int getId_Config() {
		return id_Config;
	}
	public void setId_Config(int id_Config) {
		this.id_Config = id_Config;
	}
	public String getName_Config() {
		return name_Config;
	}
	public void setName_Config(String name_Config) {
		this.name_Config = name_Config;
	}
	public String getHostName() {
		return hostName;
	}
	public void setHostName(String hostName) {
		this.hostName = hostName;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getUserNameAccount() {
		return userNameAccount;
	}
	public void setUserNameAccount(String userNameAccount) {
		this.userNameAccount = userNameAccount;
	}
	public String getPasswordAccount() {
		return passwordAccount;
	}
	public void setPasswordAccount(String passwordAccount) {
		this.passwordAccount = passwordAccount;
	}
	public String getFile_Format() {
		return file_Format;
	}
	public void setFile_Format(String file_Format) {
		this.file_Format = file_Format;
	}
	public String getRemote_Dir() {
		return remote_Dir;
	}
	public void setRemote_Dir(String remote_Dir) {
		this.remote_Dir = remote_Dir;
	}
	public String getLocal_Dir() {
		return local_Dir;
	}
	public void setLocal_Dir(String local_Dir) {
		this.local_Dir = local_Dir;
	}
	public String getName_db_Control() {
		return name_db_Control;
	}
	public void setName_db_Control(String name_db_Control) {
		this.name_db_Control = name_db_Control;
	}
	public String getName_table_config() {
		return name_table_config;
	}
	public void setName_table_config(String name_table_config) {
		this.name_table_config = name_table_config;
	}
	public String getName_table_log() {
		return name_table_log;
	}
	public void setName_table_log(String name_table_log) {
		this.name_table_log = name_table_log;
	}
	public String getName_db_Staging() {
		return name_db_Staging;
	}
	public void setName_db_Staging(String name_db_Staging) {
		this.name_db_Staging = name_db_Staging;
	}
	public String getName_table_staging() {
		return name_table_staging;
	}
	public void setName_table_staging(String name_table_staging) {
		this.name_table_staging = name_table_staging;
	}
	public String getName_db_Warehouse() {
		return name_db_Warehouse;
	}
	public void setName_db_Warehouse(String name_db_Warehouse) {
		this.name_db_Warehouse = name_db_Warehouse;
	}
	public String getName_table_warehouse() {
		return name_table_warehouse;
	}
	public void setName_table_warehouse(String name_table_warehouse) {
		this.name_table_warehouse = name_table_warehouse;
	}
}
