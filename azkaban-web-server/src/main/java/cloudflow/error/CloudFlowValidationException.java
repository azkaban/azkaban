package cloudflow.error;

public class CloudFlowValidationException extends CloudFlowException {

  public CloudFlowValidationException(String error) {
        super(error);
    }

  public CloudFlowValidationException(String error, Exception e) {
    super(error, e);
  }
}
