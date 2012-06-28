package azkaban.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.project.ProjectManager;
import azkaban.project.ResourceLoader;
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
    private HashMap<String, Props> flowProps = new HashMap<String, Props>(); 
    private ArrayList<String> errors;

    public Flow(String id) {
        this.id = id;
    }

    public void addBaseNode(Node node) {
    	this.baseNodes.add(node);
    }
    
    public void addAllNodes(Collection<Node> nodes) {
        for (Node node: nodes) {
        	addNode(node);
        }
    }
    
    public void addNode(Node node) {
        nodes.put(node.getId(), node);
    }

    public void addProperties(Props props) {
    	flowProps.put(props.getSource(), props);
    }
    
    public void addAllProperties(Collection<Props> props) {
    	for (Props prop: props) {
    		flowProps.put(prop.getSource(), prop);
    	}
    }
    
    public String getId() {
        return id;
    }
    
    public void addError(String error) {
        if (errors == null) {
            errors = new ArrayList<String>();
        }
  
        errors.add(error);
    }
    
    public List<String> getErrors() {
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
    
    public void addAllEdges(Collection<Edge> edges) {
    	for (Edge edge: edges) {
    		addEdge(edge);
    	}
    }
    
    public void addEdge(Edge edge) {
    	String source = edge.getSourceId();
    	String target = edge.getTargetId();

    	if (edge instanceof ErrorEdge) {
    		addError("Error on " + edge.getId() + ". " + ((ErrorEdge)edge).getError());
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
		flowObj.put("props", objectizeProperties());
		flowObj.put("nodes", objectizeNodes());
		flowObj.put("edges", objectizeEdges());
		if (errors != null) {
			flowObj.put("errors", errors);
		}
		
		return flowObj;
    }
    
    @SuppressWarnings("unchecked")
	public static Flow flowFromObject(Object object, ResourceLoader loader) {
    	Map<String, Object> flowObject = (Map<String,Object>)object;
    	
    	String id = (String)flowObject.get("id");
    	Flow flow = new Flow(id);
    	
    	// Loading projects
    	List<Object> propertiesList = (List<Object>)flowObject.get("props");
    	Map<String, Props> properties = loadPropertiesFromObject(propertiesList, loader);
    	flow.addAllProperties(properties.values());
    	
    	// Loading nodes
    	List<Object> nodeList = (List<Object>)flowObject.get("nodes");
    	Map<String, Node> nodes = loadNodesFromObjects(nodeList, properties, loader);
    	flow.addAllNodes(nodes.values());
    	
    	// Loading edges
    	List<Object> edgeList = (List<Object>)flowObject.get("edges");
    	List<Edge> edges = loadEdgeFromObjects(edgeList, nodes, loader);
    	flow.addAllEdges(edges);
    	
    	return flow;
    }
    
    private static Map<String, Node> loadNodesFromObjects(List<Object> nodeList, Map<String, Props> properties, ResourceLoader loader) {
    	Map<String, Node> nodeMap = new HashMap<String, Node>();
    	
    	for (Object obj: nodeList) {
    		@SuppressWarnings("unchecked")
			Map<String,Object> nodeObj = (Map<String,Object>)obj;
    		String id = (String)nodeObj.get("id");
    		String propsSource = (String)nodeObj.get("props.source");
    		String inheritedSource = (String)nodeObj.get("inherited.source");

    		Props inheritedProps = properties.get(inheritedSource);
    		Props props = loader.loadPropsFromSource(inheritedProps, propsSource);
    		
    		Node node = new Node(id, props);
    		nodeMap.put(id, node);
    	}
    	
    	return nodeMap;
    }
    
    private static List<Edge> loadEdgeFromObjects(List<Object> edgeList, Map<String, Node> nodes, ResourceLoader loader) {
    	List<Edge> edgeResult = new ArrayList<Edge>();
    	
    	for (Object obj: edgeList) {
    		@SuppressWarnings("unchecked")
			Map<String,Object> edgeObj = (Map<String,Object>)obj;
    		String id = (String)edgeObj.get("id");
    		String source = (String)edgeObj.get("source");
    		String target = (String)edgeObj.get("target");
    		
    		Node sourceNode = nodes.get(source);
    		Node targetNode = nodes.get(target);
    		String error = (String)edgeObj.get("error");
    		
    		Edge edge = null;
    		if (sourceNode == null && targetNode != null) {
    			edge = new ErrorEdge(source, target, "Edge Error: Neither source " + source + " nor " + target + " could be found.");
    		}
    		else if (sourceNode == null && targetNode != null) {
    			edge = new ErrorEdge(source, target, "Edge Error: Source " + source + " could not be found. Target: " + target);
    		}
    		else if (sourceNode != null && targetNode == null) {
    			edge = new ErrorEdge(source, target, "Edge Error: Source found " + source + ", but " + target + " could be found.");
    		}
    		else if (error != null) {
    			edge = new ErrorEdge(source, target, error);
    		}
    		else {
    			edge = new Edge(sourceNode, targetNode);
    		}
    		
    		edgeResult.add(edge);
    	}
    	
    	return edgeResult;    
    }
    
    @SuppressWarnings("unchecked")
	private static Map<String, Props> loadPropertiesFromObject(List<Object> propertyObjectList, ResourceLoader loader) {
    	Map<String, Props> properties = new HashMap<String, Props>();
    	
    	Map<String, String> sourceToInherit = new HashMap<String,String>();
    	for (Object propObj: propertyObjectList) {
    		Map<String, Object> mapObj = (Map<String,Object>)propObj;
    		String source = (String)mapObj.get("source");
    		String inherits = (String)mapObj.get("inherits");
    		
    		sourceToInherit.put(source, inherits);
    	}
    	
    	for (String source: sourceToInherit.keySet()) {
    		recursiveResolveProps(source, sourceToInherit, loader, properties);
    	}
    	
    	return properties;
    }
    
    private static void recursiveResolveProps(String source, Map<String, String> sourceToInherit, ResourceLoader loader, Map<String, Props> properties) {
    	Props prop = properties.get(source);
    	if (prop != null) {
    		return;
    	}
    	
    	String inherits = sourceToInherit.get(source);
    	Props parent = null;
    	if (inherits != null) {
    		recursiveResolveProps(inherits, sourceToInherit, loader, properties);
        	parent = properties.get(inherits);
    	}

    	prop = loader.loadPropsFromSource(parent, source);
    	properties.put(source, prop);
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
	
	private List<Map<String,Object>> objectizeProperties() {
		
		ArrayList<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
		for (Props props: flowProps.values()) {
			HashMap<String, Object> propObj = new HashMap<String, Object>();
			propObj.put("source", props.getSource());
			Props parent = props.getParent();
			if (parent != null) {
				propObj.put("inherits", parent.getSource());
			}
			result.add(propObj);
		}
		
		return result;
	}
}