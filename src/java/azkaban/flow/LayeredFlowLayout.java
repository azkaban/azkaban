package azkaban.flow;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Map;

public class LayeredFlowLayout implements FlowLayout{
	@Override
	public void layoutFlow(Flow flow) {
		analyzeFlow(flow);
		flow.setLayedOut(true);
	}

	private void analyzeFlow(Flow flow) {
		Map<String, Node> node = flow.getNodeMap();
		
	}

	private class WrappedNode extends LayeredNode {
		private Node node;
		public WrappedNode(Node node) {
			this.node = node;
		}
		public Node getNode() {
			return node;
		}
	}

	private class LayeredNode {
		private Point2D point;
		private int level;
		private ArrayList<LayeredNode> inNodes;
		private ArrayList<LayeredNode> outNodes;

		public int getLevel() {
			return level;
		}
		public void setLevel(int level) {
			this.level = level;
		}
		public Point2D getPoint() {
			return point;
		}
		public void setPoint(Point2D point) {
			this.point = point;
		}
	}
}
