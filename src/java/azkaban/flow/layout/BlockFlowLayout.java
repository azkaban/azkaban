package azkaban.flow.layout;

import java.util.HashMap;
import java.util.Set;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;

public class BlockFlowLayout implements FlowLayout {
	
	@Override
	public void layoutFlow(Flow flow) {
//		createLayeredGraph(flow);
	}

//	private void createLayeredGraph(Flow flow) {
//		HashMap<String, LayeredNode> layeredNode = new HashMap<String, LayeredNode>();
//		
//		for(Node node: flow.getStartNodes()) {
//			WrappedNode wnode = new WrappedNode(node);
//			layeredNode.put(wnode.getId(), wnode);
//			wnode.setLevel(0);
//			traverseNodes(layeredNode, flow, wnode);
//		}
//	}
//	
//	private void traverseNodes(HashMap<String, LayeredNode> layeredNode, Flow flow, LayeredNode wnode) {
//		Set<Edge> outEdges = flow.getOutEdges(wnode.getId());
//		Set<Edge> inEdges = flow.getInEdges(wnode.getId());
//		if (outEdges == null) {
//			return;
//		}
//		
//		if (outEdges.size() == 1 && inEdges != null && inEdges.size() == 1) {
//			System.out.println("Starting chain for " + wnode.getId());
//			ChainedNode cnode = new ChainedNode();
//			wnode.addOutNode(cnode);
//			cnode.addInNode(wnode);
//			
//			while(outEdges.size() == 1 && inEdges.size() == 1) {
//				Edge edge = outEdges.iterator().next();
//				if (layeredNode.containsKey(edge.getTargetId())) {
//					return;
//				}
//				
//				Node target = edge.getTarget();
//				cnode.addNode(new WrappedNode(target));
//				outEdges = flow.getOutEdges(target.getId());
//				layeredNode.put(target.getId(), cnode);
//				
//				System.out.println("Chain " + target.getId());
//
//				if (outEdges == null) {
//					return;
//				}
//				inEdges = flow.getInEdges(target.getId());
//			}
//			
//			traverseNodes(layeredNode, flow, cnode);
//		}
//		else {
//			for (Edge edge: outEdges) {
//				if (layeredNode.containsKey(edge.getTargetId())) {
//					continue;
//				}
//				
//				WrappedNode child = new WrappedNode(edge.getTarget());
//				layeredNode.put(child.getId(), child);
//				wnode.addOutNode(child);
//				child.addInNode(wnode);
//				
//				traverseNodes(layeredNode, flow, child);
//			}
//		}
//		
//	}
}
