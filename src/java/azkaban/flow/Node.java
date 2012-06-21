package azkaban.flow;

import azkaban.utils.Props;

public class Node {
    public enum State {
        FAILED, SUCCEEDED, RUNNING, WAITING, IGNORED
    }

    private final String id;

    private State state = State.WAITING;
    private Props props;
    
    public Node(String id, Props props) {
        this.id = id;
    }

    /**
     * Clones nodes
     * @param node
     */
    public Node(Node clone) {
    	this.id = clone.id;
    	this.props = clone.props;
    	this.state = clone.state;
    }

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public Props getProps() {
        return props;
    }

    public void setProps(Props props) {
        this.props = props;
    }
}