package azkaban.flow.layout2;

public class LayeredEdge {
	private LayeredNode source;
	private LayeredNode dest;
	
	public LayeredEdge(LayeredNode source, LayeredNode dest) {
		this.source = source;
		this.dest = dest;
	}
}
