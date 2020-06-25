package Hang;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.jcraft.jsch.*;

public class DownloadFile {
	private Connection connection;

	public void loadFromSourceFile() {
		System.out.println("connect success");
		try {
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery("select * from logs join config on config.id = logs.id");
			String file_name, status, src_type, delimited,source, des,user_des,pw_des,field;
			int id;

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
				
				if (src_type.equals("xlsx")) {
//					loadFromXLSX(id, status, file_name, source, des, user_des, pw_des, delimited, field);
				}
			
				
			}
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	public void downloadFtp(String userName, String password, String host, int port, String pathDir) {
		Session session = null;
		Channel channel = null;

		try {
			JSch ssh = new JSch();
			JSch.setConfig("StrictHostKeyChecking", "no");
			session = ssh.getSession(userName, host, port);
			session.setPassword(password);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftp = (ChannelSftp) channel;
			sftp.get(pathDir, "specify path to where you want the files to be output");
		} catch (JSchException e) {
			System.out.println(userName);
			System.out.println(password);
			e.printStackTrace();

		} catch (SftpException e) {
			System.out.println(userName);
			e.printStackTrace();
		} finally {
			if (channel != null) {
				((ChannelSftp) channel).disconnect();
			}
			if (session != null) {
				session.disconnect();
			}
		}

	}

	public static void main(String[] args) {
		DownloadFile download = new DownloadFile();
		String hostname = "drive.ecepvn.org";
		int port = 2227;
		String userName = "guest_access";
		String password = "123456";
		String pathDir = "F:\\HK6-2020\\DataWareHouse_ThaySong\\FileNopBai";
		download.downloadFtp(userName, password, hostname, port, pathDir);
	}
}
