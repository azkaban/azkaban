package azkaban.flow.layout;

import azkaban.flow.Node;

public class WrappedNode extends LayeredNode {
	private Node node;
	public WrappedNode(Node node) {
		this.node = node;
		setLevel(node.getLevel());
	}
	public Node getNode() {
		return node;
	}
	@Override
	public String getId() {
		return node.getId();
	}
}
