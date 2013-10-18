package azkaban.trigger;


public class TriggerException extends Exception{
	private static final long serialVersionUID = 1L;

	public TriggerException(String message) {
		super(message);
	}
	
	public TriggerException(String message, Throwable cause) {
		super(message, cause);
	}

	public TriggerException(Throwable e) {
		super(e);
	}
}

