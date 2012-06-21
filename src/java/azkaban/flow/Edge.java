package azkaban.flow;

public class Edge {
    public enum State {
        FAILED, SUCCEEDED, WAITING, CYCLE
    }

    private final Node source;
    private final Node target;

    private State state = State.WAITING;
    
    public Edge(Node from, Node to) {
        this.source = from;
        this.target = to;
    }

    public Edge(Edge clone) {
    	this.source = clone.source;
    	this.target = clone.target;
    	this.state = clone.state;
    }
    
    public String getId() {
        return getSourceId() + ">>" + getTargetId();
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public Node getSource() {
        return source;
    }

    public Node getTarget() {
        return target;
    }
    
	public String getSourceId() {
		return source == null? null : source.getId();
	}

	public String getTargetId() {
		return target == null? null : target.getId();
	}
}
