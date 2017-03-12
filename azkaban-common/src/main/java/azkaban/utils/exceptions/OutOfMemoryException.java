package azkaban.utils.exceptions;

public class OutOfMemoryException extends Exception {
  private final static long serialVersionUID = 1;

  public OutOfMemoryException(String message) {
    super(message);
  }

  public OutOfMemoryException(Throwable cause) {
    super(cause);
  }

  public OutOfMemoryException(String message, Throwable cause) {
    super(message, cause);
  }

}
