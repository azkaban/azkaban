package cloudflow.error;

import java.util.Arrays;
import java.util.List;

public class CloudFlowException extends Exception {

    // A list to avoid multiple api calls when there are various validation errors. Especially
    // useful with endpoints that take several parameters.
    private List<CloudFlowError> errors;

    public CloudFlowException(List<CloudFlowError> errors) {
        this.errors = errors;
    }

    public CloudFlowException(CloudFlowError error) {
        this.errors = Arrays.asList(error);
    }

    public List<CloudFlowError> getErrors() {
        return errors;
    }
}
