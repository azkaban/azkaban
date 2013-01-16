package azkaban.jobtype;

public class JobTypeManagerException extends RuntimeException {

//	private final static long serialVersionUID = 1;

	public JobTypeManagerException(String message) {
		super(message);
	}

	public JobTypeManagerException(Throwable cause) {
		super(cause);
	}

	public JobTypeManagerException(String message, Throwable cause) {
		super(message, cause);
	}

}
