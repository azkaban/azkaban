package azkaban.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Helper class that will find the md5 hash for files.
 */
public class Md5Hasher {
	
	private static final int BYTE_BUFFER_SIZE = 1024;
	
	public static MessageDigest getMd5Digest() {
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// Should never get here.
		}
		
		return digest;
	}
	
	public static byte[] md5Hash(File file) throws IOException {
		MessageDigest digest = getMd5Digest();
		
		FileInputStream fStream = new FileInputStream(file);
		BufferedInputStream bStream = new BufferedInputStream(fStream);
		DigestInputStream blobStream = new DigestInputStream(bStream, digest);
		
		byte[] buffer = new byte[BYTE_BUFFER_SIZE];
		
		int num = 0;
		do {
			num = blobStream.read(buffer);
		} while (num > 0);
		
		bStream.close();
		
		return digest.digest();
	}
	
}
