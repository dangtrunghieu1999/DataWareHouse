package w3.lab2.bai2;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class AES {
	private byte[] key;

	private static final String ALGORITHM = "AES";

	public AES(byte[] key) {
		this.key = key;
	}

	public byte[] encrypt(byte[] plainText) throws Exception {
		SecretKeySpec secretKey = new SecretKeySpec(key, ALGORITHM);
		Cipher cipher = Cipher.getInstance(ALGORITHM);
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);

		return cipher.doFinal(plainText);
	}

	public static void main(String[] args) throws Exception {
		byte[] encryptionKey = "MZygpewJsCpRrfOr".getBytes(StandardCharsets.UTF_8);
		byte[] plainText = "Híu Đồng Nai".getBytes(StandardCharsets.UTF_8);
		AES a1 = new AES(encryptionKey);
		byte[] cipherText = a1.encrypt(plainText);

		System.out.println(new String(plainText));
		System.out.println(new String(cipherText));

	}
}
