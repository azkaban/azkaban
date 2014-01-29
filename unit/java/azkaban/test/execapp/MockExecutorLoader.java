package azkaban.test.execapp;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableJobInfo;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutionReference;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Pair;
import azkaban.utils.Props;

public class MockExecutorLoader implements ExecutorLoader {

	HashMap<Integer, ExecutableFlow> flows = new HashMap<Integer, ExecutableFlow>();
	HashMap<String, ExecutableNode> nodes = new HashMap<String, ExecutableNode>();
	HashMap<Integer, ExecutionReference> refs = new HashMap<Integer, ExecutionReference>();
	int flowUpdateCount = 0;
	HashMap<String, Integer> jobUpdateCount = new HashMap<String,Integer>();
	Map<Integer, Pair<ExecutionReference, ExecutableFlow>> activeFlows = new HashMap<Integer, Pair<ExecutionReference,ExecutableFlow>>();
	
	@Override
	public void uploadExecutableFlow(ExecutableFlow flow) throws ExecutorManagerException {
		flows.put(flow.getExecutionId(), flow);
		flowUpdateCount++;
	}

	@Override
	public ExecutableFlow fetchExecutableFlow(int execId) throws ExecutorManagerException {
		ExecutableFlow flow = flows.get(execId);
		return ExecutableFlow.createExecutableFlowFromObject(flow.toObject());
	}

	@Override
	public Map<Integer, Pair<ExecutionReference, ExecutableFlow>> fetchActiveFlows() throws ExecutorManagerException {
		return activeFlows;
	}

	@Override
	public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId, int skip, int num) throws ExecutorManagerException {
		return null;
	}

	@Override
	public void addActiveExecutableReference(ExecutionReference ref) throws ExecutorManagerException {
		refs.put(ref.getExecId(), ref);
	}

	@Override
	public void removeActiveExecutableReference(int execId) throws ExecutorManagerException {
		refs.remove(execId);
	}
	
	public boolean hasActiveExecutableReference(int execId) {
		return refs.containsKey(execId);
	}

	@Override
	public void uploadLogFile(int execId, String name, int attempt, File... files) throws ExecutorManagerException {

	}

	@Override
	public void updateExecutableFlow(ExecutableFlow flow) throws ExecutorManagerException {
		ExecutableFlow toUpdate = flows.get(flow.getExecutionId());
		
		toUpdate.applyUpdateObject((Map<String,Object>)flow.toUpdateObject(0));
		flowUpdateCount++;
	}

	@Override
	public void uploadExecutableNode(ExecutableNode node, Props inputParams) throws ExecutorManagerException {
		ExecutableNode exNode = new ExecutableNode();
		exNode.fillExecutableFromMapObject(node.toObject());
		
		nodes.put(node.getId(), exNode);
		jobUpdateCount.put(node.getId(), 1);
	}

	@Override
	public void updateExecutableNode(ExecutableNode node) throws ExecutorManagerException {
		ExecutableNode foundNode = nodes.get(node.getId());
		foundNode.setEndTime(node.getEndTime());
		foundNode.setStartTime(node.getStartTime());
		foundNode.setStatus(node.getStatus());
		foundNode.setUpdateTime(node.getUpdateTime());
		
		Integer value = jobUpdateCount.get(node.getId());
		if (value == null) {
			throw new ExecutorManagerException("The node has not been uploaded");
		}
		else {
			jobUpdateCount.put(node.getId(), ++value);
		}
		
		flowUpdateCount++;
	}

	@Override
	public int fetchNumExecutableFlows(int projectId, String flowId) throws ExecutorManagerException {
		return 0;
	}

	@Override
	public int fetchNumExecutableFlows() throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int getFlowUpdateCount() {
		return flowUpdateCount;
	}
	
	public Integer getNodeUpdateCount(String jobId) {
		return jobUpdateCount.get(jobId);
	}

	@Override
	public ExecutableJobInfo fetchJobInfo(int execId, String jobId, int attempt)
			throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean updateExecutableReference(int execId, long updateTime) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public LogData fetchLogs(int execId, String name, int attempt, int startByte, int endByte) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ExecutableFlow> fetchFlowHistory(int skip, int num) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ExecutableFlow> fetchFlowHistory(String projectContains, String flowContains, String userNameContains, int status,
			long startData, long endData, int skip, int num) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ExecutableJobInfo> fetchJobHistory(int projectId, String jobId, int skip, int size)
			throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int fetchNumExecutableNodes(int projectId, String jobId) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Props fetchExecutionJobInputProps(int execId, String jobId) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Props fetchExecutionJobOutputProps(int execId, String jobId) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Pair<Props, Props> fetchExecutionJobProps(int execId, String jobId) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ExecutableJobInfo> fetchJobInfoAttempts(int execId, String jobId) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int removeExecutionLogsByTime(long millis)
			throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<ExecutableFlow> fetchFlowHistory(int projectId, String flowId, int skip, int num, Status status) throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Object> fetchAttachments(int execId, String name, int attempt)
			throws ExecutorManagerException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void uploadAttachmentFile(ExecutableNode node, File file)
			throws ExecutorManagerException {
		// TODO Auto-generated method stub
		
	}


}