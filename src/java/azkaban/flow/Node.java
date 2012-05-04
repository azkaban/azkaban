package azkaban.flow;

import java.util.ArrayList;
import java.util.List;

import azkaban.utils.Props;

public class Node {
    public enum State {
        FAILED, SUCCEEDED, RUNNING, WAITING, IGNORED
    }

    private final String id;
    private List<Edge> outEdges = new ArrayList<Edge>();
    private List<Edge> inEdges = new ArrayList<Edge>();
    private List<String> missingDependency;
    private State state = State.WAITING;
    private Props props;
    
    public Node(String id, Props props) {
        this.id = id;
    }

    public void addOutEdges(Edge edge) {
        outEdges.add(edge);
    }

    public void addInEdges(Edge edge) {
        inEdges.add(edge);
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

    public List<Edge> getOutEdges() {
        return outEdges;
    }

    public List<Edge> getInEdges() {
        return inEdges;
    }

    public void addMissingDependency(String dependency) {
        if (missingDependency==null) {
            missingDependency = new ArrayList<String>();
        }

        missingDependency.add(dependency);
    }

    public boolean hasMissingDependency() {
        return missingDependency != null && missingDependency.size() > 0;
    }
}