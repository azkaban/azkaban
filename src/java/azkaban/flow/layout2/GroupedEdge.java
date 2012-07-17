package azkaban.flow.layout2;

import java.util.ArrayList;

public class GroupedEdge {
	private WrappedNode source;
	private WrappedNode dest;
	private ArrayList<ChainedEdge> edges;
	
	public GroupedEdge(WrappedNode source, WrappedNode dest) {
		this.setSource(source);
		this.setDest(dest);
		this.edges = new ArrayList<ChainedEdge>();
	}
	
	public WrappedNode getSource() {
		return source;
	}
	
	public void setSource(WrappedNode source) {
		this.source = source;
	}
	
	public WrappedNode getDest() {
		return dest;
	}
	
	public void setDest(WrappedNode dest) {
		this.dest = dest;
	}
	
	public void addNestedEdge(ChainedEdge edge) {
		edges.add(edge);
	}
}
