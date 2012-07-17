package azkaban.flow.layout2;

import azkaban.flow.Node;

public class WrappedNode extends LayeredNode {
	private Node node;
	private boolean visited=false;
	public WrappedNode(Node node) {
		this.node = node;
	}
	public Node getNode() {
		return node;
	}
	@Override
	public String getId() {
		return node.getId();
	}
	public boolean isVisited() {
		return visited;
	}
	public void setVisited(boolean visited) {
		this.visited = visited;
	}

}
