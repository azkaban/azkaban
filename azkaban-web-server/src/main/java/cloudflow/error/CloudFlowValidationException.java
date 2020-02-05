package cloudflow.error;

import java.util.Arrays;
import java.util.List;

public class CloudFlowValidationException extends CloudFlowException {

    // A list to avoid multiple api calls when there are various validation errors. Especially
    // useful with endpoints that take several parameters.
    private List<String> validationErrors;

    public CloudFlowValidationException(List<String> validationErrors) {
        super("Invalid parameters.");
        this.validationErrors = validationErrors;
    }

    public CloudFlowValidationException(String validationError) {
        super("Invalid parameters.");
        this.validationErrors = Arrays.asList(validationError);
    }

    public List<String> getValidationErrors() {
        return this.validationErrors;
    }
}
