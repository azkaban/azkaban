package azkaban.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Flow {
    public enum State {
        READY, RUNNING, RUNNING_WITH_FAILURE, FAILED, SUCCEEDED
    }
    private final String id;
    private ArrayList<Node> baseNodes = new ArrayList<Node>();
    private HashMap<String, Node> nodes = new HashMap<String, Node>();
    private HashMap<String, Set<Edge>> sourceEdges = new HashMap<String, Set<Edge>>();
    private HashMap<String, Set<Edge>> targetEdges = new HashMap<String, Set<Edge>>();
    private ArrayList<Object> errors;

    public Flow(String id) {
        this.id = id;
    }

    public void addBaseNode(Node node) {
    	this.baseNodes.add(node);
    }
    
    public void addAllNodes(Collection<Node> nodes) {
        for (Node node: nodes) {
            this.nodes.put(node.getId(), node);
        }
    }
    
    public void addNode(Node node) {
        nodes.put(node.getId(), node);
    }

    public String getId() {
        return id;
    }
    
    public void addError(Object error) {
        if (errors == null) {
            errors = new ArrayList<Object>();
        }
  
        errors.add(error);
    }
    
    public List<Object> getErrors() {
    	return errors;
    }
    
    public boolean hasErrors() {
    	return errors != null && !errors.isEmpty();
    }
    
    public void addEdge(Edge edge) {
    	String source = edge.getSourceId();
    	String target = edge.getTargetId();

    	if (edge instanceof ErrorEdge) {
    		addError(edge);
    	}

    	Set<Edge> sourceSet = getEdgeSet(sourceEdges, source);
    	sourceSet.add(edge);
    	
    	Set<Edge> targetSet = getEdgeSet(targetEdges, target);
    	targetSet.add(edge);
    	
    }
    
    private Set<Edge> getEdgeSet(HashMap<String, Set<Edge>> map, String id) {
    	Set<Edge> edges = map.get(id);
    	if (edges == null) {
    		edges = new HashSet<Edge>();
    		map.put(id, edges);
    	}
    	
    	return edges;
    }
}