package w3.lab2.bai3;

import java.security.Key;
import java.security.Security;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;

public class DES {
	 private Key key;

	    public Key getKey() {
	        return key;
	    }
	    public byte []EnCrypt(byte []data){
	        key = null;
	        try {
	            //Security.addProvider(new com.sun.crypto.provider.SunJCE());
	            KeyGenerator kg = KeyGenerator.getInstance("DES");
	            Cipher cipher = Cipher.getInstance("DES");
	            key = kg.generateKey();
	            cipher.init(Cipher.ENCRYPT_MODE, key);
	            return (cipher.doFinal(data));
	        } catch (Exception e) {
	            return null;
	        }
	    }
	    public byte []DeCrypt(byte []data, Key key){
	        try {
	            //Security.addProvider(new com.sun.crypto.provider.SunJCE());
	          //  KeyGenerator kg = KeyGenerator.getInstance("DES");
	            Cipher cipher = Cipher.getInstance("DES");
	            cipher.init(Cipher.DECRYPT_MODE, key);
	            return cipher.doFinal(data);
	        } catch (Exception e) {
	            return null;
	        }
	    }
}
