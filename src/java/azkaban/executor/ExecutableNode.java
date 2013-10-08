/*
 * Copyright 2012 LinkedIn Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.flow.Node;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class ExecutableNode {
	private String jobId;
	private int executionId;
	private String type;
	private String jobPropsSource;
	private String inheritPropsSource;
	private Status status = Status.READY;
	private long startTime = -1;
	private long endTime = -1;
	private long updateTime = -1;
	private int level = 0;
	private ExecutableFlow flow;
	private Props outputProps;
	private int attempt = 0;
	private boolean paused = false;
	
	private long delayExecution = 0;

	private Set<String> inNodes = new HashSet<String>();
	private Set<String> outNodes = new HashSet<String>();
	
	// Used if proxy node
	private Integer externalExecutionId;
	private ArrayList<Attempt> pastAttempts = null;
	
	public ExecutableNode(Node node, ExecutableFlow flow) {
		jobId = node.getId();
		executionId = flow.getExecutionId();
		type = node.getType();
		jobPropsSource = node.getJobSource();
		inheritPropsSource = node.getPropsSource();
		status = Status.READY;
		level = node.getLevel();
		this.flow = flow;
	}
	
	public ExecutableNode() {
	}
	
	public void resetForRetry() {
		Attempt pastAttempt = new Attempt(attempt, startTime, endTime, status);
		attempt++;
		
		synchronized (this) {
			if (pastAttempts == null) {
				pastAttempts = new ArrayList<Attempt>();
			}
			
			pastAttempts.add(pastAttempt);
		}
		startTime = -1;
		endTime = -1;
		updateTime = System.currentTimeMillis();
		status = Status.READY;
	}
	
	public void setExecutableFlow(ExecutableFlow flow) {
		this.flow = flow;
	}
	
	public void setExecutionId(int id) {
		executionId = id;
	}

	public int getExecutionId() {
		return executionId;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String id) {
		this.jobId = id;
	}

	public void addInNode(String exNode) {
		inNodes.add(exNode);
	}

	public void addOutNode(String exNode) {
		outNodes.add(exNode);
	}

	public Set<String> getOutNodes() {
		return outNodes;
	}
	
	public Set<String> getInNodes() {
		return inNodes;
	}
	
	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}
	
	public long getDelayedExecution() {
		return delayExecution;
	}
	
	public void setDelayedExecution(long delayMs) {
		delayExecution = delayMs;
	}
	
	public Object toObject() {
		HashMap<String, Object> objMap = new HashMap<String, Object>();
		objMap.put("id", jobId);
		objMap.put("jobSource", jobPropsSource);
		objMap.put("propSource", inheritPropsSource);
		objMap.put("jobType", type);
		objMap.put("status", status.toString());
		objMap.put("inNodes", new ArrayList<String>(inNodes));
		objMap.put("outNodes", new ArrayList<String>(outNodes));
		objMap.put("startTime", startTime);
		objMap.put("endTime", endTime);
		objMap.put("updateTime", updateTime);
		objMap.put("level", level);
		objMap.put("externalExecutionId", externalExecutionId);
		objMap.put("paused", paused);
		
		if (pastAttempts != null) {
			ArrayList<Object> attemptsList = new ArrayList<Object>(pastAttempts.size());
			for (Attempt attempts : pastAttempts) {
				attemptsList.add(attempts.toObject());
			}
			objMap.put("pastAttempts", attemptsList);
		}
		
		return objMap;
	}

	@SuppressWarnings("unchecked")
	public static ExecutableNode createNodeFromObject(Object obj, ExecutableFlow flow) {
		ExecutableNode exNode = new ExecutableNode();
		
		HashMap<String, Object> objMap = (HashMap<String,Object>)obj;
		exNode.executionId = flow == null ? 0 : flow.getExecutionId();
		exNode.jobId = (String)objMap.get("id");
		exNode.jobPropsSource = (String)objMap.get("jobSource");
		exNode.inheritPropsSource = (String)objMap.get("propSource");
		exNode.type = (String)objMap.get("jobType");
		exNode.status = Status.valueOf((String)objMap.get("status"));
		
		exNode.inNodes.addAll( (List<String>)objMap.get("inNodes") );
		exNode.outNodes.addAll( (List<String>)objMap.get("outNodes") );
		
		exNode.startTime = JSONUtils.getLongFromObject(objMap.get("startTime"));
		exNode.endTime = JSONUtils.getLongFromObject(objMap.get("endTime"));
		exNode.updateTime = JSONUtils.getLongFromObject(objMap.get("updateTime"));
		exNode.level = (Integer)objMap.get("level");
		
		exNode.externalExecutionId = (Integer)objMap.get("externalExecutionId");
		
		exNode.flow = flow;
		Boolean paused = (Boolean)objMap.get("paused");
		if (paused!=null) {
			exNode.paused = paused;
		}
		
		List<Object> pastAttempts = (List<Object>)objMap.get("pastAttempts");
		if (pastAttempts!=null) {
			ArrayList<Attempt> attempts = new ArrayList<Attempt>();
			for (Object attemptObj: pastAttempts) {
				Attempt attempt = Attempt.fromObject(attemptObj);
				attempts.add(attempt);
			}
			
			exNode.pastAttempts = attempts;
		}
		
		return exNode;
	}

	@SuppressWarnings("unchecked")
	public void updateNodeFromObject(Object obj) {
		HashMap<String, Object> objMap = (HashMap<String,Object>)obj;
		status = Status.valueOf((String)objMap.get("status"));

		startTime = JSONUtils.getLongFromObject(objMap.get("startTime"));
		endTime = JSONUtils.getLongFromObject(objMap.get("endTime"));
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public String getJobPropsSource() {
		return jobPropsSource;
	}

	public String getPropsSource() {
		return inheritPropsSource;
	}

	public int getLevel() {
		return level;
	}

	public ExecutableFlow getFlow() {
		return flow;
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}

	public void setOutputProps(Props output) {
		this.outputProps = output;
	}

	public Props getOutputProps() {
		return outputProps;
	}

	public Integer getExternalExecutionId() {
		return externalExecutionId;
	}

	public void setExternalExecutionId(Integer externalExecutionId) {
		this.externalExecutionId = externalExecutionId;
	}

	public List<Attempt> getPastAttemptList() {
		return pastAttempts;
	}
	
	public int getAttempt() {
		return attempt;
	}

	public void setAttempt(int attempt) {
		this.attempt = attempt;
	}
	
	public boolean isPaused() {
		return paused;
	}
	
	public void setPaused(boolean paused) {
		this.paused = paused;
	}
	
	public List<Object> getAttemptObjects() {
		ArrayList<Object> array = new ArrayList<Object>();
		
		for (Attempt attempt: pastAttempts) {
			array.add(attempt.toObject());
		}
		
		return array;
	}
	
	
	public void updatePastAttempts(List<Object> pastAttemptsList) {
		if (pastAttemptsList == null) {
			return;
		}
		
		synchronized (this) {
			if (this.pastAttempts == null) {
				this.pastAttempts = new ArrayList<Attempt>();
			}

			// We just check size because past attempts don't change
			if (pastAttemptsList.size() <= this.pastAttempts.size()) {
				return;
			}

			Object[] pastAttemptArray = pastAttemptsList.toArray();
			for (int i = this.pastAttempts.size(); i < pastAttemptArray.length; ++i) {
				Attempt attempt = Attempt.fromObject(pastAttemptArray[i]);
				this.pastAttempts.add(attempt);
			}
		}

	}

	public static class Attempt {
		private int attempt = 0;
		private long startTime = -1;
		private long endTime = -1;
		private Status status;
		
		public Attempt(int attempt, long startTime, long endTime, Status status) {
			this.attempt = attempt;
			this.startTime = startTime;
			this.endTime = endTime;
			this.status = status;
		}
		
		public long getStartTime() {
			return startTime;
		}

		public long getEndTime() {
			return endTime;
		}
		
		public Status getStatus() {
			return status;
		}
		
		public int getAttempt() {
			return attempt;
		}
		
		public static Attempt fromObject(Object obj) {
			@SuppressWarnings("unchecked")
			Map<String, Object> map = (Map<String, Object>)obj;
			int attempt = (Integer)map.get("attempt");
			long startTime = JSONUtils.getLongFromObject(map.get("startTime"));
			long endTime = JSONUtils.getLongFromObject(map.get("endTime"));
			Status status = Status.valueOf((String)map.get("status"));
			
			return new Attempt(attempt, startTime, endTime, status);
		}
		
		public Map<String, Object> toObject() {
			HashMap<String,Object> attempts = new HashMap<String,Object>();
			attempts.put("attempt", attempt);
			attempts.put("startTime", startTime);
			attempts.put("endTime", endTime);
			attempts.put("status", status.toString());
			return attempts;
		}
	}
}
