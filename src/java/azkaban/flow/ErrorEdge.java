package azkaban.flow;

public class ErrorEdge extends Edge {
	private final String sourceId;
	private final String targetId;
	private final String error;
	
	public ErrorEdge(String source, Node target, String error) {
		super(null, target);
		this.targetId = target.getId();
		this.sourceId = source;
		this.error = error;
	}

	public ErrorEdge(Node source, String target, String error) {
		super(source, null);
		this.targetId = target;
		this.sourceId = source.getId();
		this.error = error;
	}
	
	
	public ErrorEdge(Node source, Node target, String error) {
		super(source, target);
		this.targetId = target.getId();
		this.sourceId = source.getId();
		this.error = error;
	}
	
	@Override
	public String getSourceId() {
		return sourceId;
	}

	@Override
	public String getTargetId() {
		return targetId;
	}
	
	public String getError() {
		return error;
	}

}
