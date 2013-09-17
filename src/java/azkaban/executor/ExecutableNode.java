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
import azkaban.utils.PropsUtils;

/**
 * Base Executable that nodes and flows are based.
 */
public class ExecutableNode {
	public static final String ID_PARAM = "id";
	public static final String STATUS_PARAM = "status";
	public static final String STARTTIME_PARAM = "startTime";
	public static final String ENDTIME_PARAM = "endTime";
	public static final String UPDATETIME_PARAM = "updateTime";
	public static final String INNODES_PARAM = "inNodes";
	public static final String OUTNODES_PARAM = "outNodes";
	public static final String TYPE_PARAM = "type";
	public static final String PROPS_SOURCE_PARAM = "propSource";
	public static final String JOB_SOURCE_PARAM = "jobSource";
	public static final String OUTPUT_PROPS_PARAM = "outputProps";
	
	private String id;
	private String type = null;
	private Status status = Status.READY;
	private long startTime = -1;
	private long endTime = -1;
	private long updateTime = -1;
	
	// Path to Job File
	private String jobSource; 
	// Path to top level props file
	private String propsSource;
	private Set<String> inNodes = null;
	private Set<String> outNodes = null;
	
	private Props inputProps;
	private Props outputProps;
	
	public static final String ATTEMPT_PARAM = "attempt";
	public static final String PASTATTEMPTS_PARAM = "pastAttempts";
	
	private int attempt = 0;
	private long delayExecution = 0;
	private ArrayList<ExecutionAttempt> pastAttempts = null;
	
	// Transient. These values aren't saved, but rediscovered.
	private ExecutableFlowBase parentFlow; 
	
	public ExecutableNode(Node node) {
		this.id = node.getId();
		this.jobSource = node.getJobSource();
		this.propsSource = node.getPropsSource();
	}
	
	public ExecutableNode(Node node, ExecutableFlowBase parent) {
		this(node.getId(), node.getType(), node.getJobSource(), node.getPropsSource(), parent);
	}

	public ExecutableNode(String id, String type, String jobSource, String propsSource, ExecutableFlowBase parent) {
		this.id = id;
		this.jobSource = jobSource;
		this.propsSource = propsSource;
		this.type = type;
		setParentFlow(parent);
	}
	
	public ExecutableNode() {
	}
	
	public ExecutableFlow getExecutableFlow() {
		if (parentFlow == null) {
			return null;
		}
		
		return parentFlow.getExecutableFlow();
	}
	
	public void setParentFlow(ExecutableFlowBase flow) {
		this.parentFlow = flow;
	}
	
	public ExecutableFlowBase getParentFlow() {
		return parentFlow;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public Status getStatus() {
		return status;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	public void setStatus(Status status) {
		this.status = status;
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
	
	public long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}
	
	public void addOutNode(String exNode) {
		if (outNodes == null) {
			outNodes = new HashSet<String>();
		}
		outNodes.add(exNode);
	}
	
	public void addInNode(String exNode) {
		if (inNodes == null) {
			inNodes = new HashSet<String>();
		}
		inNodes.add(exNode);
	}

	public Set<String> getOutNodes() {
		return outNodes;
	}
	
	public Set<String> getInNodes() {
		return inNodes;
	}
	
	public boolean hasJobSource() {
		return jobSource != null;
	}
	
	public boolean hasPropsSource() {
		return propsSource != null;
	}
	
	public String getJobSource() {
		return jobSource;
	}
	
	public String getPropsSource() {
		return propsSource;
	}
	
	public void setInputProps(Props input) {
		this.inputProps = input;
	}
	
	public void setOutputProps(Props output) {
		this.outputProps = output;
	}

	public Props getInputProps() {
		return this.inputProps;
	}
	
	public Props getOutputProps() {
		return outputProps;
	}
	
	public long getDelayedExecution() {
		return delayExecution;
	}
	
	public void setDelayedExecution(long delayMs) {
		delayExecution = delayMs;
	}
	
	public List<ExecutionAttempt> getPastAttemptList() {
		return pastAttempts;
	}
	
	public int getAttempt() {
		return attempt;
	}

	public void setAttempt(int attempt) {
		this.attempt = attempt;
	}
	
	public void resetForRetry() {
		ExecutionAttempt pastAttempt = new ExecutionAttempt(attempt, this);
		attempt++;
		
		synchronized (this) {
			if (pastAttempts == null) {
				pastAttempts = new ArrayList<ExecutionAttempt>();
			}
			
			pastAttempts.add(pastAttempt);
		}
		
		this.setStartTime(-1);
		this.setEndTime(-1);
		this.setUpdateTime(System.currentTimeMillis());
		this.setStatus(Status.READY);
	}
	
	public List<Object> getAttemptObjects() {
		ArrayList<Object> array = new ArrayList<Object>();
		
		for (ExecutionAttempt attempt: pastAttempts) {
			array.add(attempt.toObject());
		}
		
		return array;
	}
	
	public Map<String,Object> toObject() {
		Map<String,Object> mapObj = new HashMap<String,Object>();
		fillMapFromExecutable(mapObj);
		
		return mapObj;
	}
	
	protected void fillMapFromExecutable(Map<String,Object> objMap) {
		objMap.put(ID_PARAM, this.id);
		objMap.put(STATUS_PARAM, status.toString());
		objMap.put(STARTTIME_PARAM, startTime);
		objMap.put(ENDTIME_PARAM, endTime);
		objMap.put(UPDATETIME_PARAM, updateTime);
		objMap.put(TYPE_PARAM, type);
		objMap.put(ATTEMPT_PARAM, attempt);
		
		if (inNodes != null) {
			objMap.put(INNODES_PARAM, inNodes);
		}
		if (outNodes != null) {
			objMap.put(OUTNODES_PARAM, outNodes);
		}
		
//		if (hasPropsSource()) {
//			objMap.put(PROPS_SOURCE_PARAM, this.propsSource);
//		}
		if (hasJobSource()) {
			objMap.put(JOB_SOURCE_PARAM, this.jobSource);
		}
		
		if (outputProps != null) {
			objMap.put(OUTPUT_PROPS_PARAM, PropsUtils.toStringMap(outputProps, true));
		}
		
		if (pastAttempts != null) {
			ArrayList<Object> attemptsList = new ArrayList<Object>(pastAttempts.size());
			for (ExecutionAttempt attempts : pastAttempts) {
				attemptsList.add(attempts.toObject());
			}
			objMap.put(PASTATTEMPTS_PARAM, attemptsList);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void fillExecutableFromMapObject(Map<String,Object> objMap) {
		this.id = (String)objMap.get(ID_PARAM);
		this.status = Status.valueOf((String)objMap.get(STATUS_PARAM));
		this.startTime = JSONUtils.getLongFromObject(objMap.get(STARTTIME_PARAM));
		this.endTime = JSONUtils.getLongFromObject(objMap.get(ENDTIME_PARAM));
		this.updateTime = JSONUtils.getLongFromObject(objMap.get(UPDATETIME_PARAM));
		this.type = (String)objMap.get(TYPE_PARAM);
		this.attempt = (Integer)objMap.get(ATTEMPT_PARAM);
		
		if (objMap.containsKey(INNODES_PARAM)) {
			this.inNodes = new HashSet<String>();
			this.inNodes.addAll((List<String>)objMap.get(INNODES_PARAM));
		}
		
		if (objMap.containsKey(OUTNODES_PARAM)) {
			this.outNodes = new HashSet<String>();
			this.outNodes.addAll((List<String>)objMap.get(OUTNODES_PARAM));
		}
//	
//		if (objMap.containsKey(PROPS_SOURCE_PARAM)) {
//			this.propsSource = (String)objMap.get(PROPS_SOURCE_PARAM);
//		}
		
		if (objMap.containsKey(JOB_SOURCE_PARAM)) {
			this.jobSource = (String)objMap.get(JOB_SOURCE_PARAM);
		}
		
		if (objMap.containsKey(OUTPUT_PROPS_PARAM)) {
			this.outputProps = new Props(null, (Map<String,String>)objMap.get(OUTPUT_PROPS_PARAM));
		}
		
		List<Object> pastAttempts = (List<Object>)objMap.get(PASTATTEMPTS_PARAM);
		if (pastAttempts!=null) {
			ArrayList<ExecutionAttempt> attempts = new ArrayList<ExecutionAttempt>();
			for (Object attemptObj: pastAttempts) {
				ExecutionAttempt attempt = ExecutionAttempt.fromObject(attemptObj);
				attempts.add(attempt);
			}
			
			this.pastAttempts = attempts;
		}
	}

	public Map<String, Object> toUpdateObject() {
		Map<String, Object> updatedNodeMap = new HashMap<String,Object>();
		updatedNodeMap.put(ID_PARAM, getId());
		updatedNodeMap.put(STATUS_PARAM, getStatus().getNumVal());
		updatedNodeMap.put(STARTTIME_PARAM, getStartTime());
		updatedNodeMap.put(ENDTIME_PARAM, getEndTime());
		updatedNodeMap.put(UPDATETIME_PARAM, getUpdateTime());
		
		updatedNodeMap.put(ATTEMPT_PARAM, getAttempt());

		if (getAttempt() > 0) {
			ArrayList<Map<String,Object>> pastAttempts = new ArrayList<Map<String,Object>>();
			for (ExecutionAttempt attempt: getPastAttemptList()) {
				pastAttempts.add(attempt.toObject());
			}
			updatedNodeMap.put(PASTATTEMPTS_PARAM, pastAttempts);
		}
		
		return updatedNodeMap;
	}
	
	@SuppressWarnings("unchecked")
	public void applyUpdateObject(Map<String, Object> updateData) {
		if (updateData.containsKey(STATUS_PARAM)) {
			this.status = Status.fromInteger((Integer)updateData.get(STATUS_PARAM));
		}
		if (updateData.containsKey(STARTTIME_PARAM)) {
			this.startTime = JSONUtils.getLongFromObject(updateData.get(STARTTIME_PARAM));
		}
		if (updateData.containsKey(UPDATETIME_PARAM)) {
			this.updateTime = JSONUtils.getLongFromObject(updateData.get(UPDATETIME_PARAM));
		}
		if (updateData.containsKey(ENDTIME_PARAM)) {
			this.endTime = JSONUtils.getLongFromObject(updateData.get(ENDTIME_PARAM));
		}
		
		if (updateData.containsKey(ATTEMPT_PARAM)) {
			attempt = (Integer)updateData.get(ATTEMPT_PARAM);
			if (attempt > 0) {
				updatePastAttempts((List<Object>)updateData.get(PASTATTEMPTS_PARAM));
			}
		}
	}
	
	public void killNode(long killTime) {
		if (this.status == Status.DISABLED) {
			skipNode(killTime);
		}
		else {
			this.setStatus(Status.KILLED);
			this.setStartTime(killTime);
			this.setEndTime(killTime);
		}
	}
	
	public void skipNode(long skipTime) {
		this.setStatus(Status.SKIPPED);
		this.setStartTime(skipTime);
		this.setEndTime(skipTime);
	}
	
	private void updatePastAttempts(List<Object> pastAttemptsList) {
		if (pastAttemptsList == null) {
			return;
		}
		
		synchronized (this) {
			if (this.pastAttempts == null) {
				this.pastAttempts = new ArrayList<ExecutionAttempt>();
			}

			// We just check size because past attempts don't change
			if (pastAttemptsList.size() <= this.pastAttempts.size()) {
				return;
			}

			Object[] pastAttemptArray = pastAttemptsList.toArray();
			for (int i = this.pastAttempts.size(); i < pastAttemptArray.length; ++i) {
				ExecutionAttempt attempt = ExecutionAttempt.fromObject(pastAttemptArray[i]);
				this.pastAttempts.add(attempt);
			}
		}
	}
}
