package azkaban.flow.layout2;

import java.util.ArrayList;
import java.util.List;

import azkaban.flow.Edge;

public abstract class LayeredNode {
	private int level = -1;
	private ArrayList<ChainedEdge> inEdges;
	private ArrayList<ChainedEdge> outEdges;
	private double minX;
	private double maxX;
	private double x;
	private double y;
	
	public LayeredNode() {
		inEdges = new ArrayList<ChainedEdge>();
		outEdges = new ArrayList<ChainedEdge>();
	}
	
	public String getId() {
		return "dummy";
	}
	
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	public double getX() {
		return x;
	}
	public void setX(double x) {
		this.x = x;
	}
	public double getMinX() {
		return minX;
	}
	public void setMinX(double min) {
		minX = min;
	}
	public double getMaxX() {
		return maxX;
	}
	public void setMaxX(double max) {
		this.maxX = max;
	}
	public void setY(double y) {
		this.y = y;
	}
	public double getY() {
		return y;
	}
	
	public void addInEdge(ChainedEdge edge) {
		inEdges.add(edge);
	}
	
	public void addOutNode(ChainedEdge edge) {
		outEdges.add(edge);
	}
	
	public List<ChainedEdge> getInEdges() {
		return inEdges;
	}
	
	public List<ChainedEdge> getOutNode() {
		return outEdges;
	}
}