package azkaban.utils;

/**
 * Indicates that a base64 encoded hash string (MD5 or SHA1) is invalid (wrong length, invalid characters, etc.)
 */
public class InvalidHashException extends Exception {
  public InvalidHashException() {
    super();
  }

  public InvalidHashException(final String s) {
    super(s);
  }

  public InvalidHashException(final String s, final Exception e) {
    super(s, e);
  }
}
