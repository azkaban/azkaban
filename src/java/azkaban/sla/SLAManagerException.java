package azkaban.sla;

public class SLAManagerException extends Exception{
	private static final long serialVersionUID = 1L;

	public SLAManagerException(String message) {
		super(message);
	}
	
	public SLAManagerException(String message, Throwable cause) {
		super(message, cause);
	}
}
