package azkaban.flow;

import java.util.HashMap;

public class Edge {
	private final String sourceId;
	private final String targetId;
	private Node source;
	private Node target;
	private String error;
	

	public Edge(String fromId, String toId) {
		this.sourceId = fromId;
		this.targetId = toId;
	}

	public Edge(Edge clone) {
		this.sourceId = clone.sourceId;
		this.targetId = clone.targetId;
		this.error = clone.error;
	}
	
	public String getId() {
		return getSourceId() + ">>" + getTargetId();
	}

	public String getSourceId() {
		return sourceId;
	}

	public String getTargetId() {
		return targetId;
	}
	
	public void setError(String error) {
		this.error = error;
	}
	
	public String getError() {
		return this.error;
	}
	
	public boolean hasError() {
		return this.error != null;
	}
	
	public Node getSource() {
		return source;
	}

	public void setSource(Node source) {
		this.source = source;
	}

	public Node getTarget() {
		return target;
	}

	public void setTarget(Node target) {
		this.target = target;
	}
	
	public Object toObject() {
		HashMap<String, Object> obj = new HashMap<String, Object>();
		obj.put("source", getSourceId());
		obj.put("target", getTargetId());
		if (error != null) {
			obj.put("error", error);
		}
		
		return obj;
	}
	
	public static Edge fromObject(Object obj) {
		HashMap<String, Object> edgeObj = (HashMap<String,Object>)obj;
		
		String source = (String)edgeObj.get("source");
		String target = (String)edgeObj.get("target");
		
		String error = (String)edgeObj.get("error");
		
		Edge edge = new Edge(source, target);
		edge.setError(error);
		
		return edge;
	}

}
