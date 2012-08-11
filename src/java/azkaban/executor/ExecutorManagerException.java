package azkaban.executor;

public class ExecutorManagerException extends Exception {
	private static final long serialVersionUID = 1L;

	public ExecutorManagerException(String message) {
		super(message);
	}
	
	public ExecutorManagerException(String message, Throwable cause) {
		super(message, cause);
	}
}
