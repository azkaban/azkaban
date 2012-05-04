package azkaban.flow;

public class Edge {
    public enum State {
        FAILED, SUCCEEDED, WAITING, CYCLE
    }

    private final Node from;
    private final Node to;

    private State state = State.WAITING;
    
    public Edge(Node from, Node to) {
        this.from = from;
        this.to = to;
    }

    public String getId() {
        return from.getId() + ">>" + to.getId();
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public Node getFrom() {
        return from;
    }

    public Node getTo() {
        return to;
    }
}
