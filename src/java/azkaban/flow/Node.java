package azkaban.flow;

import java.awt.geom.Point2D;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLEngineResult.Status;

import azkaban.utils.Props;

public class Node {
	public enum State {
		FAILED, SUCCEEDED, RUNNING, WAITING, IGNORED
	}

	private final String id;
	private String jobSource;
	private String propsSource;
	private State state = State.WAITING;

	private Point2D.Double position = null;
	private int level;
	private int expectedRunTimeSec = 1;

	public Node(String id) {
		this.id = id;
	}

	/**
	 * Clones nodes
	 * @param node
	 */
	public Node(Node clone) {
		this.id = clone.id;
		this.propsSource = clone.propsSource;
		this.jobSource = clone.jobSource;
		this.state = clone.state;
	}

	public String getId() {
		return id;
	}
	
	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public Point2D.Double getPosition() {
		return position;
	}

	public void setPosition(Point2D.Double position) {
		this.position = position;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}
	
	public String getJobSource() {
		return jobSource;
	}

	public void setJobSource(String jobSource) {
		this.jobSource = jobSource;
	}

	public String getPropsSource() {
		return propsSource;
	}

	public void setPropsSource(String propsSource) {
		this.propsSource = propsSource;
	}
	
	public void setExpectedRuntimeSec(int runtimeSec) {
		expectedRunTimeSec = runtimeSec;
	}
	
	public int getExpectedRuntimeSec() {
		return expectedRunTimeSec;
	}
	
	@SuppressWarnings("unchecked")
	public static Node fromObject(Object obj) {
		Map<String,Object> mapObj = (Map<String,Object>)obj;
		String id = (String)mapObj.get("id");
		
		Node node = new Node(id);
		String jobSource = (String)mapObj.get("job.source");
		String propSource = (String)mapObj.get("prop.source");
		node.setJobSource(jobSource);
		node.setPropsSource(propSource);
		
		Integer expectedRuntime = (Integer)mapObj.get("expectedRuntime");
		if (expectedRuntime != null) {
			node.setExpectedRuntimeSec(expectedRuntime);
		}

		String stateStr = (String)mapObj.get("status");
		if (stateStr != null) {
			State state = State.valueOf(stateStr);
			if (state != null) {
				node.setState(state);
			}
		}
		
		Map<String,Object> layoutInfo = (Map<String,Object>)mapObj.get("layout");
		if (layoutInfo != null) {
			Double x = (Double)layoutInfo.get("x");
			Double y = (Double)layoutInfo.get("y");
			Integer level = (Integer)layoutInfo.get("level");
			
			if (x != null && y != null) {
				node.setPosition(new Point2D.Double(x, y));
			}
			if (level != null) {
				node.setLevel(level);
			}
		}
		
		return node;
	}
	
	public Object toObject() {
		HashMap<String, Object> objMap = new HashMap<String, Object>();
		objMap.put("id", id);
		objMap.put("job.source", jobSource);
		objMap.put("prop.source", propsSource);
		objMap.put("expectedRuntime", expectedRunTimeSec);
		objMap.put("state", state.toString());

		if (position != null) {
			HashMap<String, Object> layoutInfo = new HashMap<String, Object>();
			layoutInfo.put("x", position.x);
			layoutInfo.put("y", position.y);
			layoutInfo.put("level", level);
			objMap.put("layout", layoutInfo);
		}
		
		return objMap;
	}
}