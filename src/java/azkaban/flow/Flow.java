package azkaban.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.utils.Props;

public class Flow {
    public enum State {
        READY, RUNNING, RUNNING_WITH_FAILURE, FAILED, SUCCEEDED
    }
    private final String id;
    private ArrayList<Node> baseNodes = new ArrayList<Node>();
    private HashMap<String, Node> nodes = new HashMap<String, Node>();
    private HashMap<String, Edge> edges = new HashMap<String, Edge>();
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
    
    public Collection<Node> getNodes() {
    	return nodes.values();
    }
    
    public Collection<Edge> getEdges() {
    	return edges.values();
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
    	
    	edges.put(edge.getId(), edge);
    }
    
    private Set<Edge> getEdgeSet(HashMap<String, Set<Edge>> map, String id) {
    	Set<Edge> edges = map.get(id);
    	if (edges == null) {
    		edges = new HashSet<Edge>();
    		map.put(id, edges);
    	}
    	
    	return edges;
    }
    
    public Map<String,Object> toObject() {
		HashMap<String, Object> flowObj = new HashMap<String, Object>();
		flowObj.put("type", "flow");
		flowObj.put("id", getId());
		flowObj.put("properties", objectizeProperties());
		flowObj.put("nodes", objectizeNodes());
		flowObj.put("edges", objectizeEdges());
		
		return flowObj;
    }
    
	private List<Map<String,Object>> objectizeNodes() {
		ArrayList<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
		for (Node node : getNodes()) {
			HashMap<String, Object> nodeObj = new HashMap<String, Object>();
			nodeObj.put("id", node.getId());
			nodeObj.put("props.source", node.getProps().getSource());
			Props parentProps = node.getProps().getParent();
			
			if (parentProps != null) {
				nodeObj.put("inherited.source", parentProps.getSource());
			}
			result.add(nodeObj);
		}
		
		return result;
	}
	
	private List<Map<String,Object>> objectizeEdges() {
		ArrayList<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
		for (Edge edge: getEdges()) {
			HashMap<String, Object> edgeObj = new HashMap<String, Object>();
			edgeObj.put("id", edge.getId());
			edgeObj.put("source", edge.getSourceId());
			edgeObj.put("target", edge.getTargetId());
			if (edge instanceof ErrorEdge) {
				ErrorEdge errorEdge = (ErrorEdge)edge;
				edgeObj.put("error", errorEdge.getError());
			}
			result.add(edgeObj);
		}
		
		return result;
	}
	
	@SuppressWarnings("unchecked")
	private List<Map<String,Object>> objectizeProperties() {
		ArrayList<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
		
		HashMap<String, Object> properties = new HashMap<String, Object>();
		for (Node node: getNodes()) {
			Props props = node.getProps().getParent();
			if (props != null) {
				traverseAndObjectizeProperties(properties, props);
			}
		}
		
		for (Object propMap : properties.values()) {
			result.add((Map<String,Object>)propMap);
		}
		
		return result;
	}
	
	private void traverseAndObjectizeProperties(HashMap<String, Object> properties, Props props) {
		if (props.getSource() == null || properties.containsKey(props.getSource())) {
			return;
		}
		
		HashMap<String, Object> propObj = new HashMap<String,Object>();
		propObj.put("source", props.getSource());
		properties.put(props.getSource(), propObj);
		
		Props parent = props.getParent();
		if (parent != null) {
			propObj.put("inherits", parent.getSource());
			
			traverseAndObjectizeProperties(properties, parent);
		}
	}
}