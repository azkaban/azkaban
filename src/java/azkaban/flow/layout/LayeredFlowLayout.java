package azkaban.flow.layout;

import java.awt.geom.Point2D;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import azkaban.flow.Edge;
import azkaban.flow.Flow;
import azkaban.flow.Node;

public class LayeredFlowLayout implements FlowLayout{
	private static final double EPSILON = 0.00000001;
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
		
		trueMidUpDown(nodeLayers, levelWithMax);

		printLayer(flow.getId(), nodeLayers, maxNodesInLevel, levelWithMax);
		flow.setLayedOut(true);
	}

	private void assignPointsToFlowNodes(ArrayList<ArrayList<LayeredNode>> nodeLayers, double xScale, double minXSpacing) {
		// This is going top down.
		HashMap<String, ArrayList<Point2D>> edgeGuidePoints = new HashMap<String, ArrayList<Point2D>>();
		HashMap<String, Edge> edgeMap = new HashMap<String, Edge>();
		for (ArrayList<LayeredNode> layer: nodeLayers) {
			for (LayeredNode lnode : layer) {
				if (lnode instanceof WrappedNode) {
					WrappedNode wnode = (WrappedNode)lnode;
					Node node = wnode.getNode();
					node.setPosition(wnode.getX()*xScale, lnode.getLevel() * minXSpacing);
				}
				else if (lnode instanceof DummyNode){
					DummyNode dnode = (DummyNode)lnode;
					Edge edge = dnode.getEdge();
					String id = edge.getId();
					
					ArrayList<Point2D> guides = edgeGuidePoints.get(id);
					if (guides == null) {
						guides = new ArrayList<Point2D>();
						edgeGuidePoints.put(id, guides);
						edgeMap.put(id, edge);
					}
					
					Point2D point = new Point2D.Double(dnode.getX()*xScale, dnode.getLevel() * minXSpacing);
					guides.add(point);
				}
			}
		}
		
		for (Edge edge: edgeMap.values()) {
			String id = edge.getId();
			ArrayList<Point2D> guides = edgeGuidePoints.get(id);
			
			if (guides != null) {
				ArrayList<Point2D> filteredList = null;
				if (guides.size() == 1) {
					filteredList = guides;
				}
				else {
					filteredList = new ArrayList<Point2D>();
					
					// Add first
					Point2D first = guides.get(0);
					filteredList.add(first);
					double lastX = first.getX();
					
					for(int i=1; i < filteredList.size() - 1; ++i) {
						Point2D dummyPoint = filteredList.get(i);
						double currentX = dummyPoint.getX();
						if (Math.abs(lastX - currentX) < EPSILON) {
							continue;
						}
						
						lastX = currentX;
						filteredList.add(dummyPoint);
					}
					
					// Add last
					filteredList.add(guides.get(guides.size() - 1));
				}
				
				edge.setGuides("dummyNodes", filteredList);
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
			
			// Reset top position
			//intializeLevel(nodeLayers.get(0), null);
			
			// Going from the first item unwrapping downward
			min = Math.min(min, uncrossLayers(nodeLayers, 1, nodeLayers.size() - 1, comparator));
		}
		else if (nodeLayers.size() > 1) {
			min = uncrossLayers(nodeLayers, 1, 1, comparator);
		}
		
		System.out.println("min:" + min);
		double scale = min > 0 ? MIN_X_SPACING / min : MIN_X_SPACING;
		scale = Math.min(scale, 50);
	
		assignPointsToFlowNodes(nodeLayers, scale, MIN_Y_SPACING);
	}
	
	// Lays out by the longest row item, up... then do all the whole thing.
	private void trueMidUpDown(ArrayList<ArrayList<LayeredNode>> nodeLayers, int levelWithMax) {
		LevelComparator comparator = new LevelComparator();
		
		if (levelWithMax == 0) {
			midUpDownScheme(nodeLayers, levelWithMax);
			return;
		}
		
		ArrayList<LayeredNode> level = nodeLayers.get(levelWithMax);
		Collections.sort(level, comparator);
		
		Random random = new Random(1);
		intializeLevel(level, random);
		double min = Double.MAX_VALUE;
		
		if (nodeLayers.size() > 2) {
			// Going from the last item unwrapping upwards
			min = Math.min(min, uncrossLayers(nodeLayers, levelWithMax - 1, 0, comparator));
			
			// Reset top position
			//intializeLevel(nodeLayers.get(0), null);
			
			// Going from the first item unwrapping downward
			min = Math.min(min, uncrossLayers(nodeLayers, 1, nodeLayers.size() - 1, comparator));
		}
		else if (nodeLayers.size() > 1) {
			min = uncrossLayers(nodeLayers, 1, 1, comparator);
		}
		
		System.out.println("min:" + min);
		double scale = min > 0 ? MIN_X_SPACING / min : MIN_X_SPACING;
		scale = Math.min(scale, 50);
	
		assignPointsToFlowNodes(nodeLayers, scale, MIN_Y_SPACING);
	}
	
	private void intializeLevel(ArrayList<LayeredNode> layers, Random random) {
		double starting = 0;
		for (LayeredNode node: layers) {
			node.setX(starting);
			node.setMaxX(starting + 0.5);
			node.setMinX(starting - 0.5);

			if (random  != null) {
				// Why random hopping? between 0.5 to 1.0
				double randomHop = random.nextDouble();
				starting += (1 + randomHop*0.5);
			}
			else {
				starting += 1;
			}
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
			// What we're attempting here is to gain more room.
			// If the previous placed node is a dummy node, then we try to get closer to it without
			// overlapping it. Otherwise, we split the difference.
			LayeredNode previousNode = layer.get(startIndex -1);
			double factor = previousNode instanceof DummyNode ? 0.3 : 0.5;
			startSplit = layer.get(startIndex).getX()*factor + previousNode.getX()*(1.0 - factor);
		}

		if (endIndex == layer.size() - 1) {
			endSplit = layer.get(endIndex).getMaxX();
		}
		else {
			LayeredNode nextNode = layer.get(endIndex + 1);
			double factor = nextNode instanceof DummyNode ? 0.3 : 0.5;
			endSplit = layer.get(endIndex).getX()*factor + nextNode.getX()*(1.0-factor);
		}
		
		double deltaDiff = endSplit - startSplit;
		if (deltaDiff < EPSILON) {
			System.err.println("WTH It's 0!!");
		}
		else {
			int length = endIndex - startIndex + 1;
			// Assign weights of 0.25 to dummies, and 1 to regular. Then apply left and right.
			ArrayList<LayeredNode> regulars = new ArrayList<LayeredNode>(length);
			ArrayList<LayeredNode> dummies = new ArrayList<LayeredNode>(length);

			for (int i = startIndex; i <= endIndex; ++i) {
				LayeredNode node = layer.get(i);
				if (node instanceof DummyNode) {
					dummies.add(node);
				}
				else if (node instanceof WrappedNode) {
					regulars.add(node);
				}
			}
			
			double start = startSplit;
			double end = endSplit;
			double weightedCount = (0.4*dummies.size() + (double)regulars.size()) - 1.0;
			double regularStep = deltaDiff / weightedCount;
			double dummyStep = regularStep * 0.3;
			
			int dummyFront = dummies.size() / 2;
			for (LayeredNode node : dummies) {
				node.setX(start);
				start += dummyStep;
			}
			
			for (LayeredNode node : regulars) {
				node.setX(start);
				start += regularStep;
			}
//			
//			// We do this to place the regular nodes in the middle and the dummy nodes off to the side.
//			int dummyFront = dummies.size() / 2;
//			for (int i = 0; i < dummyFront; ++i) {
//				LayeredNode node = dummies.get(i);
//				node.setX(start);
//				start += dummyStep;
//			}
//			for (int i = dummyFront; i < dummies.size(); ++i) {
//				LayeredNode node = dummies.get(i);
//				node.setX(end);
//				end -= dummyStep;
//			}
//			
//			for (LayeredNode node: regulars) {
//				node.setX(start);
//				start += regularStep;
//			}
//			
			return regularStep;
			
//			// startIndex - endIndex should be at least 2.
//			double step = deltaDiff / (double)(endIndex - startIndex);
//			double start = startSplit;
//			for (int i = startIndex; i <= endIndex; ++i) {
//				LayeredNode node = layer.get(i);
//				node.setX(start);
//				start += step;
//			}
			
//			return step;
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
			int destLevel = dest.getLevel();
			
			for (int index = sourceLevel + 1; index < destLevel; index++) {
				DummyNode dummyNode = new DummyNode(edge);
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
