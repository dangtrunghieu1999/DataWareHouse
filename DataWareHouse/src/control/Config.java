package control;

public class Config {
	/* Id của Configuration */
	private int idConf;
	/* Thư mục chứa File trên FTP */
	private String remoteDir;
	/* Thứ mục chứa file khi download file về ở Local */
	private String localDir;
	/* Tên host ở đây à FTP */
	private String hostName;
	/* Port FTP */
	private int port;
	/* Tên User đăng nhập FTP */
	private String userHost;
	/* Password để đăng nhập FTP */
	private String userPass;
	/* Các trường có trong file CSV */
	private String columsHostFeed;
	/* Các trường trong STAGING sẽ được load vào trong WAREHOUSE */
	private String propsStagingForWareHouse;
	/* Các trường trong WAREHOUSE */
	private String propsWarehouse;
	/* Delimiter của nguồn dữ liệu ở đây là CSV,Xlxs */
	private String feedDelimiter;
	/* Tên nguồn dữ liệu Ví dụ: Chieu_nhom06 */
	private String srcFeed;
	/* Tên table STAGING */
	private String tableStaging;
	/* Định dạng ngày của nguồn dữ liệu */
	private String dateFormat;
	/* Gmail đại diện của nhóm */
	private String mailGroup;
	/* Script để extract & transform data*/
	private String transformScript;
	
	public void setIdConf(int idConf) {
		this.idConf = idConf;
	}
	
	public int getIdConf() {
		return idConf;
	}

	public String getRemoteDir() {
		return remoteDir;
	}

	public void setRemoteDir(String remoteDir) {
		this.remoteDir = remoteDir;
	}
	
	public void setLocalDir(String localDir) {
		this.localDir = localDir;
	}
	
	public String getLocalDir() {
		return localDir;
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

	public String getUserHost() {
		return userHost;
	}

	public void setUserHost(String userHost) {
		this.userHost = userHost;
	}

	public String getUserPass() {
		return userPass;
	}

	public void setUserPass(String userPass) {
		this.userPass = userPass;
	}
	
	public void setColumsHostFeed(String columsHostFeed) {
		this.columsHostFeed = columsHostFeed;
	}
	
	public String getColumsHostFeed() {
		return columsHostFeed;
	}

	public void setPropsStagingForWareHouse(String propsStagingForWareHouse) {
		this.propsStagingForWareHouse = propsStagingForWareHouse;
	}
	
	public String getPropsStagingForWareHouse() {
		return propsStagingForWareHouse;
	}

	public String getPropsWarehouse() {
		return propsWarehouse;
	}

	public void setPropsWarehouse(String propsWarehouse) {
		this.propsWarehouse = propsWarehouse;
	}

	public String getFeedDelimiter() {
		return feedDelimiter;
	}

	public void setFeedDelimiter(String feedDelimiter) {
		this.feedDelimiter = feedDelimiter;
	}

	public String getSrcFeed() {
		return srcFeed;
	}

	public void setSrcFeed(String srcFeed) {
		this.srcFeed = srcFeed;
	}

	public String getTableStaging() {
		return tableStaging;
	}

	public void setTableStaging(String tableStaging) {
		this.tableStaging = tableStaging;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getMailGroup() {
		return mailGroup;
	}

	public void setMailGroup(String mailGroup) {
		this.mailGroup = mailGroup;
	}
	
	public void setTransformScript(String transformScript) {
		this.transformScript = transformScript;
	}
	
	public String getTransformScript() {
		return transformScript;
	}
}
