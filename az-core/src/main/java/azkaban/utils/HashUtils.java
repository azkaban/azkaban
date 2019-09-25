/*
 * Copyright 2019 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import static java.nio.charset.StandardCharsets.*;


/**
 * Helper class that can find a hash for a file or string.
 */

public enum HashUtils {
  MD5("MD5"),
  SHA1("SHA1");

  private final String type;

  private static final int BYTE_BUFFER_SIZE = 1024;

  HashUtils(final String type) {
    this.type = type;
  }

  public String getName() {
    return type;
  }

  private MessageDigest getDigest() {
    MessageDigest digest = null;
    try {
      digest = MessageDigest.getInstance(getName());
    } catch (final NoSuchAlgorithmException e) {
      // Should never get here.
    }
    return digest;
  }

  public String getHashStr(final String str) {
    return bytesHashToString(getHashBytes(str)).toLowerCase();
  }

  public byte[] getHashBytes(final String str) {
    final MessageDigest digest = getDigest();
    digest.update(str.getBytes(UTF_8));
    return digest.digest();
  }

  public String getHashStr(final File file) throws IOException {
    return bytesHashToString(getHashBytes(file)).toLowerCase();
  }

  public byte[] getHashBytes(final File file) throws IOException {
    final MessageDigest digest = getDigest();

    final FileInputStream fStream = new FileInputStream(file);
    final BufferedInputStream bStream = new BufferedInputStream(fStream);
    final DigestInputStream blobStream = new DigestInputStream(bStream, digest);

    final byte[] buffer = new byte[BYTE_BUFFER_SIZE];

    int num = 0;
    do {
      num = blobStream.read(buffer);
    } while (num > 0);

    bStream.close();

    return digest.digest();
  }

  /**
   * Validates and sanitizes a hash string. Ensures the hash does not include any non-alphanumeric characters
   * and ensures it is the correct length for its type. If the hash is valid, a lowercase version is returned.
   *
   * @param raw raw hash string
   * @return lowercase raw hash string
   * @throws InvalidHashException if the hash is invalid for any of the reasons described above.
   */
  public String sanitizeHashStr(final String raw) throws InvalidHashException {
    if (!raw.matches("^[a-zA-Z0-9]*$")) {
      throw new InvalidHashException(
          String.format("Hash %s has invalid characters. Should be only alphanumeric.", raw));
    } else if (this.type.equals("MD5") && raw.length() != 32) {
      throw new InvalidHashException(
          String.format("MD5 hash %s has incorrect length %d, expected 32", raw, raw.length()));
    } else if (this.type.equals("SHA1") && raw.length() != 40) {
      throw new InvalidHashException(
          String.format("SHA1 hash %s has incorrect length %d, expected 40", raw, raw.length()));
    }
    return raw.toLowerCase();
  }

  public static boolean isSameHash(final String a, final byte[] b) throws DecoderException {
    return isSameHash(stringHashToBytes(a), b);
  }

  public static boolean isSameHash(final byte[] a, final byte[] b) {
    return Arrays.equals(a, b);
  }

  public static byte[] stringHashToBytes(final String a) throws DecoderException {
    return Hex.decodeHex(a.toCharArray());
  }

  public static String bytesHashToString(final byte[] a) {
    return String.valueOf(Hex.encodeHex(a)).toLowerCase();
  }
}
