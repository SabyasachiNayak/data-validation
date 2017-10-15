package org.com.sabs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

public class HashGenerator {
	final static Logger logger = Logger.getLogger(HashGenerator.class.getName());
	public static String generateHash(byte[] s) {
		byte[] mdbyte = null;
		try {
			mdbyte = MessageDigest.getInstance("MD5").digest(s);
		}
		catch (NoSuchAlgorithmException e) {
			logger.error(e.getMessage());
		}
		StringBuffer hashCode = new StringBuffer();
		for (int i = 0; i < mdbyte.length; i++) {
			hashCode.append(Integer.toString((mdbyte[i] & 0xff) + 0x100, 16).substring(1));
		}
		
		logger.info("hashcode generated");
		return hashCode.toString();
	}
}
