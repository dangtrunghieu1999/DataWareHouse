package model;

import java.sql.Date;

public class Log {
	private int id;
	private String nameLog;
	private String comment;
	private String status;
	private String urlLocal;
	
	private String time_download;
	private String time_uploadStraging;
	private String time_Warehouse;
	
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}
	public String getNameLog() {
		return nameLog;
	}
	public void setNameLog(String nameLog) {
		this.nameLog = nameLog;
	}

	
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getUrlLocal() {
		return urlLocal;
	}
	public void setUrlLocal(String urlLocal) {
		this.urlLocal = urlLocal;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getTime_download() {
		return time_download;
	}
	public void setTime_download(String time_download) {
		this.time_download = time_download;
	}
	public String getTime_uploadStraging() {
		return time_uploadStraging;
	}
	public void setTime_uploadStraging(String time_uploadStraging) {
		this.time_uploadStraging = time_uploadStraging;
	}
	public String getTime_Warehouse() {
		return time_Warehouse;
	}
	public void setTime_Warehouse(String time_Warehouse) {
		this.time_Warehouse = time_Warehouse;
	}

}
