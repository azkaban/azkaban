package azkaban.flow.layout2;

import java.util.ArrayList;

public class ChainedEdge {
	private LayeredNode start;
	private LayeredNode end;
	private ArrayList<LayeredNode> chain;
	private String chainString = null;
	
	public ChainedEdge(LayeredNode start, LayeredNode end) {
		this.start = start;
		this.end = end;
		chain = new ArrayList<LayeredNode>();
	}
	
	public LayeredNode getStart() {
		return start;
	}
	
	public LayeredNode getEnd() {
		return end;
	}
	
	public void addChain(ArrayList<LayeredNode> chain) {
		this.chain = chain;
		StringBuffer chainString = new StringBuffer();
		chainString.append(start.getId());
		
		
		chainString.append(end.getId());
	}
	
	public String chainString() {
		return chainString;
	}
}
