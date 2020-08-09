package common;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.Email;
import org.apache.commons.mail.SimpleEmail;

public class SendEmail {
	public static String send(String title, String mess) {
		String result = "";
		try {
			Email email = new SimpleEmail();

			// Cấu hình thông tin Email Server
			email.setHostName("smtp.googlemail.com");
			email.setSmtpPort(465);
			email.setAuthenticator(new DefaultAuthenticator(SystemContain.MY_EMAIL, SystemContain.MY_PASSWORD));

			// Với gmail cái này là bắt buộc.
			email.setSSLOnConnect(true);

			// Người gửi
			email.setFrom(SystemContain.MY_EMAIL);

			// Tiêu đề
			email.setSubject(title);

			// Nội dung email
			email.setMsg(mess);

			// Người nhận
			email.addTo(SystemContain.FRIEND_EMAIL);
			email.send();
			result = "success";
		} catch (Exception e) {
			result = "failed";
		}
		return result;
	}
}
