package azkaban.flow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class LayeredFlowLayout implements FlowLayout{
	private static final double EPSILON = 0.000001;
	private static final double MIN_X_SPACING = 1.0;
	private static final double MIN_Y_SPACING = 1.0;
	
	@Override
	public void layoutFlow(Flow flow) {
		ArrayList<ArrayList<LayeredNode>> nodeLayers = setupFlow(flow);
		
		int maxNodesInLevel = 0;
		int levelWithMax = 0;
		int index = 0;
		for (ArrayList<LayeredNode> layer : nodeLayers) {

			int num = layer.size();
			if (num < maxNodesInLevel) {
				maxNodesInLevel = num;
				levelWithMax = index;
			}
			index++;
		}
		
		midUpDownScheme(nodeLayers, levelWithMax);

		printLayer(flow.getId(), nodeLayers, maxNodesInLevel, levelWithMax);
		flow.setLayedOut(true);
	}

	private void assignPointsToFlowNodes(ArrayList<ArrayList<LayeredNode>> nodeLayers, double xScale, double minXSpacing) {
		for (ArrayList<LayeredNode> layer: nodeLayers) {
			for (LayeredNode lnode : layer) {
				if (lnode instanceof WrappedNode) {
					WrappedNode wnode = (WrappedNode)lnode;
					Node node = wnode.getNode();
					node.setPosition(wnode.getX()*xScale, lnode.level * minXSpacing);
				}
			}
		}
	}
	
	// Lays out by the longest row item, up... then do all the whole thing.
	private void midUpDownScheme(ArrayList<ArrayList<LayeredNode>> nodeLayers, int levelWithMax) {
		LevelComparator comparator = new LevelComparator();
		
		//ArrayList<LayeredNode> level = nodeLayers.get(levelWithMax);
		ArrayList<LayeredNode> level = nodeLayers.get(nodeLayers.size() - 1);
		Collections.sort(level, comparator);
		
		Random random = new Random(1);
		intializeLevel(level, random);
		double min = Double.MAX_VALUE;
		
		if (nodeLayers.size() > 2) {
			// Going from the last item unwrapping upwards
			min = Math.min(min, uncrossLayers(nodeLayers, nodeLayers.size() - 2, 0, comparator));
			
			// Going from the first item unwrapping downward
			min = Math.min(min, uncrossLayers(nodeLayers, 1, nodeLayers.size() - 1, comparator));
		}
		else if (nodeLayers.size() > 1) {
			min = uncrossLayers(nodeLayers, 1, 1, comparator);
		}
		
		System.out.println("min:" + min);
		double scale = min > 0 ? MIN_X_SPACING / min : MIN_X_SPACING;
		scale = Math.max(scale, 1);
		
		assignPointsToFlowNodes(nodeLayers, scale, MIN_Y_SPACING);
	}
	
	private void intializeLevel(ArrayList<LayeredNode> layers, Random random) {
		double starting = 0;
		for (LayeredNode node: layers) {
			node.setX(starting);
			node.setMaxX(starting + 0.5);
			node.setMinX(starting - 0.5);

			// Why random hopping? between 0.5 to 1.0
			double randomHop = random.nextDouble();
			starting += (1 + randomHop*0.5);
		}
	}

	private double uncrossLayers(ArrayList<ArrayList<LayeredNode>> layers, int from, int to, LevelComparator comparator) {
		double minDistance = Double.MAX_VALUE;

		if (from < to) {
			for (int i = from; i <= to; ++i) {
				// Uncross layer
				ArrayList<LayeredNode> layer = layers.get(i);
				uncrossLayerFromIn(layer);
				// Sort layer
				Collections.sort(layer, comparator);
				minDistance = Math.min(minDistance, separateLevels(layer));
			}
		}
		else if (to < from){
			for (int i = from; i >= to; --i) {
				// Uncross layer
				ArrayList<LayeredNode> layer = layers.get(i);
				uncrossLayerFromOut(layer);
				// Sort layer
				Collections.sort(layer, comparator);
				minDistance = Math.min(minDistance, separateLevels(layer));
			}
		}
		else {
			uncrossLayerFromIn(layers.get(from));
		}
		
		return minDistance;
	}
	
	private double separateLevels(ArrayList<LayeredNode> layer) {
		int startIndex = -1;
		int endIndex = -1;
		
		if (layer.size() == 1) {
			return Double.MAX_VALUE;
		}
		
		double xPrev = Double.NaN;
		double xCurrent = Double.NaN;
		
		double minDistance = Double.MAX_VALUE;
		
		for (int i = 0; i < layer.size(); ++i) {
			LayeredNode node = layer.get(i);
			xCurrent = node.getX();
			if (Double.isNaN(xPrev)) {
				xPrev = xCurrent;
				continue;
			}
			
			double delta = xCurrent - xPrev;
			if (delta < EPSILON) {
				if (startIndex == -1) {
					startIndex = i - 1;
				}
				endIndex = i;
			}
			else {
				if (startIndex != -1) {
					minDistance = Math.min(separateRange(layer, startIndex, endIndex), minDistance);
					// Reset
					startIndex = -1;
					endIndex = -1;
				}
				else {
					minDistance = Math.min(delta, minDistance);
				}
			}
			
			xPrev = xCurrent;
		}
		
		// Finish it off
		if (startIndex != -1) {
			minDistance = Math.min(separateRange(layer, startIndex, layer.size() - 1), minDistance);
		}
		
		return minDistance;
	}
	
	// Range is inclusive
	private double separateRange(ArrayList<LayeredNode> layer, int startIndex, int endIndex) {
		double startSplit = 0;
		double endSplit = 0;
		if (startIndex == 0) {
			startSplit = layer.get(0).getMinX();
		}
		else {
			startSplit = (layer.get(startIndex).getX() + layer.get(startIndex - 1).getX())/2.0;
		}

		if (endIndex == layer.size() - 1) {
			endSplit = layer.get(endIndex).getMaxX();
		}
		else {
			endSplit = (layer.get(endIndex + 1).getX() + layer.get(endIndex).getX())/2.0;
		}
		
		double deltaDiff = endSplit - startSplit;
		if (deltaDiff < EPSILON) {
			System.err.println("WTH It's 0!!");
		}
		else {
			// startIndex - endIndex should be at least 2.
			double step = deltaDiff / (double)(endIndex - startIndex);
			double start = startSplit;
			for (int i = startIndex; i <= endIndex; ++i) {
				LayeredNode node = layer.get(i);
				node.setX(start);
				start += step;
			}
			
			return step;
		}

		return Double.NaN;
	}

	//Return the scale
	private void uncrossLayerFromIn(ArrayList<LayeredNode> layer) {
		for (LayeredNode node: layer) {
			double xSum = 0;
			double minX = Double.POSITIVE_INFINITY;
			double maxX = Double.NEGATIVE_INFINITY;
			int count = 0;
			
			for (LayeredNode upperLayer : node.getInNode()) {
				minX = Math.min(minX, upperLayer.getMinX());
				maxX = Math.max(maxX, upperLayer.getMaxX());
				xSum += upperLayer.getX();
				
				count++;
			}
			
			if (count == 0) {
				System.err.println("This is not right");
			}
			else {
				double x = count == 0 ? 0 : xSum / (double)count;
				node.setX(x);
				node.setMaxX(maxX);
				node.setMinX(minX);
			}
		}
	}
	
	private void uncrossLayerFromOut(ArrayList<LayeredNode> layer) {
		for (LayeredNode node: layer) {
			double xSum = 0;
			double minX = Double.POSITIVE_INFINITY;
			double maxX = Double.NEGATIVE_INFINITY;
			int count = 0;
			
			
			for (LayeredNode lowerLayer : node.getOutNode()) {
				minX = Math.min(minX, lowerLayer.getMinX());
				maxX = Math.max(maxX, lowerLayer.getMaxX());
				xSum += lowerLayer.getX();
				
				count++;
			}
			
			if (count == 0) {
				System.err.println("This is not right");
			}
			else {
				double x = xSum / (double)count;
				node.setX(x);
				node.setMaxX(maxX);
				node.setMinX(minX);
			}
		}
	}
	
	private ArrayList<ArrayList<LayeredNode>> setupFlow(Flow flow) {
		Map<String, Node> nodesMap = flow.getNodeMap();
		int levels = flow.getNumLevels();

		ArrayList<ArrayList<LayeredNode>> nodeLevels = new ArrayList<ArrayList<LayeredNode>>();
		for (int i = 0; i < levels + 1; ++i) {
			nodeLevels.add(new ArrayList<LayeredNode>());
		}

		HashMap<String, WrappedNode> layeredNodeMap = new HashMap<String, WrappedNode>();
		for (Node node: nodesMap.values()) {
			int level = node.getLevel();
			WrappedNode wNode = new WrappedNode(node);
			layeredNodeMap.put(node.getId(), wNode);

			ArrayList<LayeredNode> nodeList = nodeLevels.get(level);
			nodeList.add(wNode);
		}
		
		// Adding edges and dummy nodes.
		for(Edge edge : flow.getEdges()) {
			if (edge.hasError()) {
				continue;
			}

			LayeredNode source = layeredNodeMap.get(edge.getSourceId());
			LayeredNode dest = layeredNodeMap.get(edge.getTargetId());
			int sourceLevel = source.getLevel();
			int destLevel = source.getLevel();
			
			for (int index = sourceLevel + 1; index < destLevel; index++) {
				LayeredNode dummyNode = new LayeredNode();
				dummyNode.setLevel(index);
				ArrayList<LayeredNode> nodeList = nodeLevels.get(index);
				nodeList.add(dummyNode);

				source.addOutNode(dummyNode);
				dummyNode.addInNode(source);
				source = dummyNode;
			}
			source.addOutNode(dest);
			dest.addInNode(source);
		}
		
		return nodeLevels;
	}
	
	private class WrappedNode extends LayeredNode {
		private Node node;
		public WrappedNode(Node node) {
			this.node = node;
			super.level = node.getLevel();
		}
		public Node getNode() {
			return node;
		}
		@Override
		public String getId() {
			return node.getId();
		}
	}

	private class LayeredNode {
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
	
	private void printLayer(String flowName, ArrayList<ArrayList<LayeredNode>> nodeLayers, int maxNodesInLevel, int levelWithMax) {
		System.out.println("Layout " + flowName);
		System.out.println("  Max Width " + maxNodesInLevel);
		System.out.println("  Level with max " + levelWithMax);
		int index = 0;
		for (ArrayList<LayeredNode> lnodes: nodeLayers) {
			StringBuffer nodeStr = new StringBuffer();
			nodeStr.append("  ");
			nodeStr.append(index);
			nodeStr.append(" [");
			for (LayeredNode node: lnodes) {
				nodeStr.append(node.getId());
				nodeStr.append(",");
			}
			nodeStr.append("]");
			index++;
			System.out.println(nodeStr);
		}
		
	}
	
	private class LevelComparator implements Comparator<LayeredNode> {

		@Override
		public int compare(LayeredNode o1, LayeredNode o2) {
			Double x1 = o1.getX();
			Double x2 = o2.getX();
			
			return x1.compareTo(x2);
		}
	}
}
