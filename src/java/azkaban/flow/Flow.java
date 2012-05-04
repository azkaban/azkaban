package azkaban.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class Flow {
    public enum State {
        READY, RUNNING, RUNNING_WITH_FAILURE, FAILED, SUCCEEDED
    }
    private final String id;
    private HashMap<String, Node> nodes = new HashMap<String, Node>();
    private HashMap<String, Edge> edges = new HashMap<String, Edge>();
    private ArrayList<String> errors = null;

    public Flow(String id) {
        this.id = id;
    }

    public void addAllNodes(Collection<Node> nodes) {
        for (Node node: nodes) {
            this.nodes.put(node.getId(), node);
        }
    }

    public void addAllEdges(Collection<Edge> edges) {
        for (Edge edge: edges) {
            this.edges.put(edge.getId(), edge);
        }
    }
    
    public void addNode(Node node) {
        nodes.put(node.getId(), node);
    }

    public void addEdge(Edge edge) {
        edges.put(edge.getId(), edge);
    }

    public String getId() {
        return id;
    }
    
    public void addErrors(String error) {
        if (errors == null) {
            errors = new ArrayList<String>();
        }
  
        errors.add(error);
    }
}