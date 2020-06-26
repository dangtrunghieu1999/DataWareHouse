package Hang;

import com.jcraft.jsch.*;

public class DownloadFile {

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
		String pathDir = "F:\\HK6-2020\\DataWareHouse_ThaySong\\FileNopBai\\sinhvien_chieu_nhom6.xlsx";
		download.downloadFtp(userName, password, hostname, port, pathDir);
	}
}
