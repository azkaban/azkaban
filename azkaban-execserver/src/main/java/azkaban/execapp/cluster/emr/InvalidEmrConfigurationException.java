package azkaban.execapp.cluster.emr;

/**
 * Created by jsoumet on 2/9/16 for azkaban.
 */
public class InvalidEmrConfigurationException extends Exception {


    public InvalidEmrConfigurationException() {
        super();
    }
    public InvalidEmrConfigurationException(String message) {
        super(message);
    }
}
