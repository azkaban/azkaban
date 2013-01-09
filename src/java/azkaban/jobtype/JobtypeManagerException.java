package azkaban.jobtype;

public class JobtypeManagerException extends RuntimeException {

//	private final static long serialVersionUID = 1;

	public JobtypeManagerException(String message) {
		super(message);
	}

	public JobtypeManagerException(Throwable cause) {
		super(cause);
	}

	public JobtypeManagerException(String message, Throwable cause) {
		super(message, cause);
	}

}
