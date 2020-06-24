package w3.lab2.bai2;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

public class DES {
	private SecretKey key;

	public SecretKey creatKey() throws NoSuchAlgorithmException {
		KeyGenerator keyGenerator = KeyGenerator.getInstance("DES");
		keyGenerator.init(56);
		key = keyGenerator.generateKey();
		return key;

	}

	public byte[] encrypt(String text) throws Exception {
		if (key == null) {
			return new byte[] {};
		}
		Cipher cipher = Cipher.getInstance("DES");
		cipher.init(Cipher.ENCRYPT_MODE, key);
		byte[] plainText = text.getBytes("UTF-8");
		byte[] cipherText = cipher.doFinal(plainText);
		return cipherText;
	}

	public String decrypt(byte[] text) throws Exception {
		if (key == null) {
			return null;
		}
		Cipher cipher = Cipher.getInstance("DES");
		cipher.init(Cipher.DECRYPT_MODE, key);
		byte[] plainText = cipher.doFinal(text);
		String output = new String(plainText, "UTF-8");
		return output;
	}

	public SecretKey getKey() {
		return this.key;
	}

	public static void main(String[] args) throws Exception {
		DES des = new DES();
		SecretKey key = des.creatKey();
		byte[] out = des.encrypt("Hiu dep trai vl");
		System.out.println(new String(out));
		System.out.println(des.decrypt(out));
	}
}
