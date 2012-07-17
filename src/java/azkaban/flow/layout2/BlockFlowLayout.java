package azkaban.flow.layout2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.flow.layout.FlowLayout;

public class BlockFlowLayout implements FlowLayout {
	
	@Override
	public void layoutFlow(Flow flow) {
		HashMap<String, LayeredNode> layoutNodes = new HashMap<String, LayeredNode>();
		
		for (Node node: flow.getNodes()) {
			layoutNodes.put(node.getId(), new WrappedNode(node));
		}
		
		for(Node node: flow.getStartNodes()) {
			WrappedNode wnode = new WrappedNode(node);
			traverseAndChain(layoutNodes, flow, wnode);
		}
	}
	
	public void traverseAndChain(HashMap<String, LayeredNode> layoutNodes, Flow flow, LayeredNode node) {
		if (layoutNodes.containsKey(node.getId())) {
			return;
		}
		
		layoutNodes.put(node.getId(), node);
		
		HashMap<String, GroupedEdge> groupEdge = new HashMap<String, GroupedEdge>();
		for(Edge edge: flow.getOutEdges(node.getId())) {
			Node dest = edge.getTarget();
			Set<Edge> destOutEdge = flow.getOutEdges(dest.getId());
			Set<Edge> destInEdge = flow.getInEdges(dest.getId());

			ArrayList<Node> chainDest = new ArrayList<Node>();
			while(destOutEdge != null && destOutEdge.size()==1 && destInEdge.size()==1) {
				chainDest.add(dest);
				dest = edge.getTarget();
				destOutEdge = flow.getOutEdges(dest.getId());
				destInEdge = flow.getInEdges(dest.getId());
			}

			
		}
		
	}

}
