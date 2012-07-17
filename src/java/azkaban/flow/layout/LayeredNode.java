package azkaban.flow.layout;

import java.util.ArrayList;
import java.util.List;

public abstract class LayeredNode {
	private int level;
	private ArrayList<LayeredNode> inNodes;
	private ArrayList<LayeredNode> outNodes;
	private double minX;
	private double maxX;
	private double x;
	private double y;
	
	public LayeredNode() {
		inNodes = new ArrayList<LayeredNode>();
		outNodes = new ArrayList<LayeredNode>();
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
	
	public void addInNode(LayeredNode node) {
		inNodes.add(node);
	}
	
	public void addOutNode(LayeredNode node) {
		outNodes.add(node);
	}
	
	public List<LayeredNode> getInNode() {
		return inNodes;
	}
	
	public List<LayeredNode> getOutNode() {
		return outNodes;
	}
}