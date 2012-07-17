package azkaban.flow.layout2;

import java.util.ArrayList;
import java.util.HashMap;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;

public class NestedLayoutGraph {
	private Flow flow;
	private HashMap<String, LayeredNode> nodes = new HashMap<String, LayeredNode>();
	private HashMap<String, LayeredEdge> edges = new HashMap<String, LayeredEdge>();
	private HashMap<String, ArrayList<LayeredEdge>> inEdges;
	private HashMap<String, ArrayList<LayeredEdge>> outEdges;
	
	public NestedLayoutGraph(Flow flow) {
		this.flow = flow;
	}
	
	private void setupGraph() {
		for (Node node: flow.getNodes()) {
			nodes.put(node.getId(), new WrappedNode(node));
		}
		
		for (Edge edge: flow.getEdges()) {
			LayeredEdge ledge = new LayeredEdge(nodes.get(edge.getSourceId()), nodes.get(edge.getTargetId()));
			edges.put(edge.getId(), ledge);
			
			//inEdges.put(edge.getSourceId(), ledge);
			//outEdges.put(edge.getTargetId(), ledge);
		}
	}
}
