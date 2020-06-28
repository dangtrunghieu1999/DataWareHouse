package Hang;

//import com.chilkatsoft.CkGlobal;
//import com.chilkatsoft.CkScp;
//import com.chilkatsoft.CkSsh;
import com.chilkatsoft.*;

public class DownloadFile {
	static {
		try {
			System.loadLibrary("chilkat");
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}

	public static void main(String argv[]) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("NguyenThiThuyHang");// mã mở khóa
		// Nó sẽ chứa chuỗi chính xác được truyền cho nó và sẽ cung cấp chi tiết về lý
		// do tại sao nó thất bại.

		String hostname = "drive.ecepvn.org";
		int port = 2227;

		// Kết nối tới SSH server:
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		// Đợi tối đa 5s khi đọc phản hồi
		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw("guest_access", "123456");
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
		scp.put_SyncMustMatch("sinhvien*.*");
		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
		String localPath = "C:\\Users\\Thuy Hang\\Downloads\\FileNopBai";

		success = scp.SyncTreeDownload(remotePath, localPath, port, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		System.out.println("SCP download file success.");
		ssh.Disconnect();
	}
}
