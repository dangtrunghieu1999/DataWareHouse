package b1_donwload;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class FileDownLoad {
	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static void DownloadFile(String hostname, int port, String user_name, String password, String remote_Path,
			String local_Path, String regex_Match) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Nhom 6");// mã mở khóa
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw(user_name, password);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		scp.put_SyncMustMatch(regex_Match);// download tất cả các file bắt đầu bằng sinhvien_chieu
		success = scp.SyncTreeDownload(remote_Path, local_Path, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		ssh.Disconnect();

	}

	public static void main(String[] args) {
		FileDownLoad.DownloadFile("drive.ecepvn.org", 2227, "guest_access", "123456",
				"/volume1/ECEP/song.nguyen/DW_2020/data", "F:\\HK6-2020\\DataWareHouse_ThaySong\\SCP_DownLoad",
				"sinhvien*.*.xlsx");
	}
}
