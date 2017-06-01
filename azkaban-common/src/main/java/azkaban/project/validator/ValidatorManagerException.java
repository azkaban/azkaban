package azkaban.project.validator;

public class ValidatorManagerException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public ValidatorManagerException(final String message) {
    super(message);
  }

  public ValidatorManagerException(final Throwable cause) {
    super(cause);
  }

  public ValidatorManagerException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
