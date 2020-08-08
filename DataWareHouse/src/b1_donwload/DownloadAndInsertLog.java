package b1_donwload;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import common.SendEmail;
import common.SystemContain;
import model.Config;
import model.Log;

public class DownloadAndInsertLog {

	public static void downloadAndInsertLog(Config config, Connection conn) throws SQLException {
		download(config);
		insertLog(config, conn);
	}

	private static boolean download(Config config) {

		// Gọi thư viện chilkat
		try {
			System.load("C:/Users\\WIN10/Desktop/chilkat-9.5.0-jdk8-x64/chilkat.dll");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
		// buoc thu 1

		CkGlobal glob = new CkGlobal();
		glob.UnlockBundle("Waiting . . .");
		CkSsh ssh = new CkSsh();
		/*
		 * 1. Kết nối máy chủ SSH:
		 * 
		 * 1.1 Lấy port, host name trong config
		 * 
		 */
		// - Lấy host name
		String hostname = config.getHostName();
		// - lấy port
		int port = config.getPort();
		// 1.2 Kết nối
		boolean success = ssh.Connect(hostname, port);
		// 1.3 kiểm tra nếu kết nối ssh lỗi thì gửi mail trả về false dừng download
		if (success == false) {
			String title = "Ket noi SSH bi loi";
			String mess = "Ket noi SSH voi hostname = " + hostname + ", port = " + port + " bi loi";
			SendEmail.send(title, mess);
			return false;
		}

		// Đợi tối đa 5 giây khi đọc phản hồi..
		ssh.put_IdleTimeoutMs(5000);

		/*
		 * 2. Xác thực bằng đăng nhập / mật khẩu: 2.1 Lấy User name, password trong
		 * Config
		 */
		// - Lấy user
		String user = config.getUserNameAccount();
		// - Lấy pass
		String pass = config.getPasswordAccount();
		// 2.2 Xác thực
		success = ssh.AuthenticatePw(user, pass);
		if (success != true) {
			String title = "Xac thuc tai khoan bi loi";
			String mess = "Xac thuc tai khoan voi user = " + user + ", password = " + pass + " bi loi";
			SendEmail.send(title, mess);
			return false;
		}
		// Khi SSH được kết nối và xác thực. Sử dụng nó
		// Trong SCP.
		CkScp scp = new CkScp();
		success = scp.UseSsh(ssh);
		if (success != true) {
			String title = "Khong the tao SCP";
			String mess = "Khong the tao SCP";
			SendEmail.send(title, mess);
			return false;
		}

		// buoc 2
		/*
		 * 3. Download file 3.1 Lấy định dạng file
		 */
		String fileFormat[] = config.getFile_Format().split(",");

		// Dựa vào định dạng file để Download
		for (int i = 0; i < fileFormat.length; i++) {
			// thêm định dạng file vào SCP
			scp.put_SyncMustMatch(fileFormat[i]);
			String remoteDir = config.getRemote_Dir();

			// buoc 3
			String localDir = config.getLocal_Dir();
			// Download synchronization modes:
			// mode=0: Download tất cả file
			// mode=1: Download tất cả các tệp không tồn tại trên hệ thống tệp cục bộ.
			// mode=2: Download các tệp mới hơn hoặc không tồn tại.
			// mode=3: Download chỉ các tập tin mới hơn.
			// mode=5: Download chỉ thiếu các tập tin hoặc tập tin với sự khác biệt kích
			// thước.
			// mode=6: Tương tự như mode 5, nhưng cũng tải xuống các tệp mới hơn.
			int mode = 0;

			// 3.2 Bắt đầu Download
			success = scp.SyncTreeDownload(remoteDir, localDir, mode, false);
			// 3.3 Kiểm tra nếu download thất bại thì gửi mail và return false
			if (success != true) {
				String title = "Download file that bai";
				String mess = "Khong the download file " + fileFormat[i] + " voi remoteDir = " + remoteDir
						+ ", localDir = " + localDir;
				SendEmail.send(title, mess);
				return false;
			}
		}
		// Ngắt kết nối SSH
		ssh.Disconnect();
		return true;
	}

	private static void insertLog(Config config, Connection conn) {
		// 4. Thêm vào log

		String localFile = config.getLocal_Dir();

		File folder = new File(localFile);
		File[] listOfFiles = folder.listFiles();

		for (File file : listOfFiles) {
			if (file.isFile()) {
				Log log = new Log();
				log.setNameLog(config.getName_Config());
				log.setUrlLocal(localFile + File.separator + file.getName());
				log.setStatus(SystemContain.DOWNLOAD_SUCCESS);
				log.setComment("file "+file.getName() +" da duoc down thanh cong");
				
				Date nowDay = new Date();
				DateFormat forMatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				log.setTime_download(forMatDate.format(nowDay));
				log.setTime_uploadStraging(null);
				log.setTime_Warehouse(null);

				String nameDB = config.getName_db_Control();
				String tableDB = config.getName_table_log();
				String sql = "insert into " + nameDB + "." + tableDB
						+ " (name_log, urlLocal, status, comment ,time_download,time_uploadStaging , time_uploadWarehouse) values (?, ?, ?, ?, ?, ?, ?)";
				PreparedStatement ps;
				try {
					ps = conn.prepareStatement(sql);
					ps.setString(1, log.getNameLog());
					ps.setString(2, log.getUrlLocal());
					ps.setString(3, log.getStatus());
					ps.setString(4, log.getComment());
					ps.setString(5, log.getTime_download());
					ps.setString(6, log.getTime_uploadStraging());
					ps.setString(7, log.getTime_Warehouse());

					ps.execute();
				} catch (SQLException e) {
				}
			}
		}

		
	
	}

}
