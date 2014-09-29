package azkaban.project.validator;

public class ValidatorManagerException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public ValidatorManagerException(String message) {
    super(message);
  }

  public ValidatorManagerException(Throwable cause) {
    super(cause);
  }

  public ValidatorManagerException(String message, Throwable cause) {
    super(message, cause);
  }

}
