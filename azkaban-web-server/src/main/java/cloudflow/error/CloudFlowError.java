package cloudflow.error;

public class CloudFlowError {

    private Enum<ErrorCode> code;
    private String message;

    public CloudFlowError(ErrorCode code, String message) {
        this.code = code;
        this.message = message;
    }
}
