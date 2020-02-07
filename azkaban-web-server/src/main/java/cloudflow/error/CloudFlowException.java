package cloudflow.error;

public class CloudFlowException extends RuntimeException {

  public CloudFlowException(String message) {
    super(message);
  }

  public CloudFlowException(String message, Throwable e) {
    super(message, e);
  }

}
