package azkaban.flow.layout;

import azkaban.flow.Edge;

public class DummyNode extends LayeredNode {
	private Edge edge;
	public DummyNode(Edge edge) {
		super();
		this.setEdge(edge);
	}
	public Edge getEdge() {
		return edge;
	}
	public void setEdge(Edge edge) {
		this.edge = edge;
	}
}