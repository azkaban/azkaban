package azkaban.flow;

public class NoSuchAzkabanResourceException extends RuntimeException {

  public NoSuchAzkabanResourceException(final String message) {
    super(message);
  }

  public NoSuchAzkabanResourceException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public NoSuchAzkabanResourceException(final Exception e) {
    super(e);
  }

}
