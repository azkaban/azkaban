package azkaban.jobtype;

public class JobTypeManagerException extends RuntimeException {
	private static final long serialVersionUID = 1L;

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
