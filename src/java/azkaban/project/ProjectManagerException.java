package azkaban.project;

public class ProjectManagerException extends Exception{
	private static final long serialVersionUID = 1L;
	public enum Type {
		
	}

	public ProjectManagerException(String message) {
		super(message);
	}
	
	public ProjectManagerException(String message, Throwable cause) {
		super(message, cause);
	}
}
