package azkaban.utils;

/**
 * Indicates that a dependency failed to successfully upload or download.
 */
public class DependencyTransferException extends RuntimeException {
  public DependencyTransferException() {
    super();
  }

  public DependencyTransferException(final String s) {
    super(s);
  }

  public DependencyTransferException(final String s, final Throwable e) {
    super(s, e);
  }
}
