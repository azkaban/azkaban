package azkaban.utils;

/**
 * Typically used when validation of a file checksum after downloading fails due to a checksum mismatch.
 */
public class HashNotMatchException extends Exception {
  public HashNotMatchException() {
    super();
  }

  public HashNotMatchException(final String s) {
    super(s);
  }

  public HashNotMatchException(final String s, final Exception e) {
    super(s, e);
  }
}
