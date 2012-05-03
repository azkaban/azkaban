package azkaban.user;

/**
 * Exception for the UserManager to capture login errors.
 * 
 */
public class UserManagerException extends Exception {
	private static final long serialVersionUID = 1L;

	public UserManagerException(String message) {
		super(message);
	}
	
	public UserManagerException(String message, Throwable cause) {
		super(message, cause);
	}
}
