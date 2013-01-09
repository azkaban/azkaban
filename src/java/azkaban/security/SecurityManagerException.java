package azkaban.security;

public class SecurityManagerException extends Exception {
	private static final long serialVersionUID = 1L;
	public SecurityManagerException(String message) {
		super(message);
	}
	
	public SecurityManagerException(String message, Throwable cause) {
		super(message, cause);
	}
}

