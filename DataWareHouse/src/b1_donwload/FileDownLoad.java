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

	public static String downloadFile() {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Team 6 ......................");
		// 1. Connect source
		int port = 2227;
		// Connect to an SSH server and establish the SSH:
		boolean success = ssh.Connect(DBConnection.hostName, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return "FAIL";
		}
		ssh.put_IdleTimeoutMs(5000);
		// 2. authentication: user && pass
		success = ssh.AuthenticatePw(DBControl.username, DBControl.password);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return "FAIL";
		}
		CkScp scp = new CkScp();
		// 3. open port ssh to go
		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return "FAIL";
		}
		// 4. dowload file in local
		scp.put_SyncMustMatch("sinhvien*.*");// down tat ca cac file bat dau
												// bang sinhvien
		// 5. 2: file was already exits not down continute
		success = scp.SyncTreeDownload(DBControl.remotePath, DBControl.localPath, 2, false);
		//

		if (success != true) {
			System.out.println(scp.lastErrorText());
			return "FAIL";
		}

		System.out.println("SCP dowload file success");
		ssh.Disconnect();

		return "SUCCESS";
	}

}
