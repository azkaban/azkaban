/*
 * Copyright 2012 LinkedIn, Inc
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

import java.io.IOException;
import java.lang.Thread.State;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import azkaban.project.Project;
import azkaban.scheduler.ScheduleStatisticManager;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;

/**
 * Executor manager used to manage the client side job.
 *
 */
public class ExecutorManagerRemoteAdapter implements ExecutorManagerAdapter {
	private static Logger logger = Logger.getLogger(ExecutorManagerRemoteAdapter.class);
	private ExecutorLoader executorLoader;
	private String executorHost;
	private int executorPort;
	private String executorManagerHost;
	private int executorManagerPort;
	
	private ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> runningFlows = new ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>>();
	private ConcurrentHashMap<Integer, ExecutableFlow> recentlyFinished = new ConcurrentHashMap<Integer, ExecutableFlow>();

	private UpdaterThread updater;
	
	private long lastThreadCheckTime = -1;
	
	public ExecutorManagerRemoteAdapter(Props props, ExecutorLoader loader) throws ExecutorManagerException {
		this.executorLoader = loader;
		this.loadRunningFlows();
		
		executorHost = props.getString("executor.host", "localhost");
		executorPort = props.getInt("executor.port");
		
		executorManagerHost = props.getString(REMOTE_EXECUTOR_MANAGER_HOST);
		executorManagerPort = props.getInt(REMOTE_EXECUTOR_MANAGER_PORT);
		
		updater = new UpdaterThread();
		updater.start();

	}
	
	@Override
	public State getExecutorManagerThreadState() {
		List<Pair<String, String>> params = new ArrayList<Pair<String,String>>();
		params.add(new Pair<String, String>(INFO_JMX_TYPE, "getExecutorManagerThreadState"));
		Map<String, Object> response;
		try {
			response = callRemoteExecutorManager(ACTION_GET_JMX, "azkaban", params);
			return (State) response.get(INFO_JMX_DATA);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	@Override
	public boolean isExecutorManagerThreadActive() {
		List<Pair<String, String>> params = new ArrayList<Pair<String,String>>();
		params.add(new Pair<String, String>(INFO_JMX_TYPE, "isExecutorManagerThreadActive"));
		Map<String, Object> response;
		try {
			response = callRemoteExecutorManager(ACTION_GET_JMX, "azkaban", params);
			return (Boolean) response.get(INFO_JMX_DATA);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
	}
	
	@Override
	public long getLastExecutorManagerThreadCheckTime() {
		List<Pair<String, String>> params = new ArrayList<Pair<String,String>>();
		params.add(new Pair<String, String>(INFO_JMX_TYPE, "getLastExecutorManagerThreadCheckTime"));
		Map<String, Object> response;
		try {
			response = callRemoteExecutorManager(ACTION_GET_JMX, "azkaban", params);
			return (Long) response.get(INFO_JMX_DATA);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}	
	}
	
	@Override
	public Set<String> getPrimaryServerHosts() {
		// Only one for now. More probably later.
		HashSet<String> ports = new HashSet<String>();
		ports.add(executorHost + ":" + executorPort);
		return ports;
	}
	
	@Override
	public Set<String> getAllActiveExecutorServerHosts() {
		// Includes non primary server/hosts
		HashSet<String> ports = new HashSet<String>();
		ports.add(executorHost + ":" + executorPort);
		for(Pair<ExecutionReference, ExecutableFlow> running: runningFlows.values()) {
			ExecutionReference ref = running.getFirst();
			ports.add(ref.getHost() + ":" + ref.getPort());
		}
		
		return ports;
	}
	
//	private ExecutableFlow fetchExecutableFlow(int execId) throws ExecutorManagerException {
//		ExecutableFlow exflow = executorLoader.fetchExecutableFlow(execId);
//		return exflow;
//	}
	
	private void loadRunningFlows() throws ExecutorManagerException {
		runningFlows.putAll(executorLoader.fetchActiveFlows());
	}
	
	@Override
	public List<Integer> getRunningFlows(int projectId, String flowId) {
		ArrayList<Integer> executionIds = new ArrayList<Integer>();
		for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
			if (ref.getSecond().getFlowId().equals(flowId)) {
				executionIds.add(ref.getFirst().getExecId());
			}
		}
		return executionIds;
	}
	
	@Override
	public boolean isFlowRunning(int projectId, String flowId) {
		for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
			if (ref.getSecond().getProjectId() == projectId && ref.getSecond().getFlowId().equals(flowId)) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public ExecutableFlow getExecutableFlow(int execId) throws ExecutorManagerException {
		Pair<ExecutionReference, ExecutableFlow> active = runningFlows.get(execId);
		if (active == null) {
			return executorLoader.fetchExecutableFlow(execId);
		}
		return active.getSecond();
	}
	
	@Override
	public List<ExecutableFlow> getRunningFlows() {
		ArrayList<ExecutableFlow> flows = new ArrayList<ExecutableFlow>();
		for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
			flows.add(ref.getSecond());
		}
		return flows;
	}
	
	@Override
	public List<ExecutableFlow> getRecentlyFinishedFlows() {
		return new ArrayList<ExecutableFlow>(recentlyFinished.values());
	}
	
	@Override
	public List<ExecutableFlow> getExecutableFlows(Project project, String flowId, int skip, int size) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(project.getId(), flowId, skip, size);
		return flows;
	}
	
	@Override
	public List<ExecutableFlow> getExecutableFlows(int skip, int size) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(skip, size);
		return flows;
	}
	
	@Override
	public List<ExecutableFlow> getExecutableFlows(String flowIdContains, int skip, int size) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(null, '%'+flowIdContains+'%', null, 0, -1, -1 , skip, size);
		return flows;
	}

	@Override
	public List<ExecutableFlow> getExecutableFlows(String projContain, String flowContain, String userContain, int status, long begin, long end, int skip, int size) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(projContain, flowContain, userContain, status, begin, end , skip, size);
		return flows;
	}
	
	@Override
	public List<ExecutableJobInfo> getExecutableJobs(Project project, String jobId, int skip, int size) throws ExecutorManagerException {
		List<ExecutableJobInfo> nodes = executorLoader.fetchJobHistory(project.getId(), jobId, skip, size);
		return nodes;
	}
	
	@Override
	public int getNumberOfJobExecutions(Project project, String jobId) throws ExecutorManagerException{
		return executorLoader.fetchNumExecutableNodes(project.getId(), jobId);
	}
	
	@Override
	public int getNumberOfExecutions(Project project, String flowId) throws ExecutorManagerException{
		return executorLoader.fetchNumExecutableFlows(project.getId(), flowId);
	}
	
	@Override
	public LogData getExecutableFlowLog(ExecutableFlow exFlow, int offset, int length) throws ExecutorManagerException {
		Pair<ExecutionReference, ExecutableFlow> pair = runningFlows.get(exFlow.getExecutionId());
		if (pair != null) {
			Pair<String,String> typeParam = new Pair<String,String>("type", "flow");
			Pair<String,String> offsetParam = new Pair<String,String>("offset", String.valueOf(offset));
			Pair<String,String> lengthParam = new Pair<String,String>("length", String.valueOf(length));
			
			@SuppressWarnings("unchecked")
			Map<String, Object> result = callExecutorServer(pair.getFirst(), ConnectorParams.LOG_ACTION, typeParam, offsetParam, lengthParam);
			return LogData.createLogDataFromObject(result);
		}
		else {
			LogData value = executorLoader.fetchLogs(exFlow.getExecutionId(), "", 0, offset, length);
			return value;
		}
	}
	
	@Override
	public LogData getExecutionJobLog(ExecutableFlow exFlow, String jobId, int offset, int length, int attempt) throws ExecutorManagerException {
		Pair<ExecutionReference, ExecutableFlow> pair = runningFlows.get(exFlow.getExecutionId());
		if (pair != null) {
			Pair<String,String> typeParam = new Pair<String,String>("type", "job");
			Pair<String,String> jobIdParam = new Pair<String,String>("jobId", jobId);
			Pair<String,String> offsetParam = new Pair<String,String>("offset", String.valueOf(offset));
			Pair<String,String> lengthParam = new Pair<String,String>("length", String.valueOf(length));
			Pair<String,String> attemptParam = new Pair<String,String>("attempt", String.valueOf(attempt));
			
			@SuppressWarnings("unchecked")
			Map<String, Object> result = callExecutorServer(pair.getFirst(), ConnectorParams.LOG_ACTION, typeParam, jobIdParam, offsetParam, lengthParam, attemptParam);
			return LogData.createLogDataFromObject(result);
		}
		else {
			LogData value = executorLoader.fetchLogs(exFlow.getExecutionId(), jobId, attempt, offset, length);
			return value;
		}
	}
	
	@Override
	public JobMetaData getExecutionJobMetaData(ExecutableFlow exFlow, String jobId, int offset, int length, int attempt) throws ExecutorManagerException {
		Pair<ExecutionReference, ExecutableFlow> pair = runningFlows.get(exFlow.getExecutionId());
		if (pair != null) {

			Pair<String,String> typeParam = new Pair<String,String>("type", "job");
			Pair<String,String> jobIdParam = new Pair<String,String>("jobId", jobId);
			Pair<String,String> offsetParam = new Pair<String,String>("offset", String.valueOf(offset));
			Pair<String,String> lengthParam = new Pair<String,String>("length", String.valueOf(length));
			Pair<String,String> attemptParam = new Pair<String,String>("attempt", String.valueOf(attempt));
			
			@SuppressWarnings("unchecked")
			Map<String, Object> result = callExecutorServer(pair.getFirst(), ConnectorParams.METADATA_ACTION, typeParam, jobIdParam, offsetParam, lengthParam, attemptParam);
			return JobMetaData.createJobMetaDataFromObject(result);
		}
		else {
			return null;
		}
	}
	
	@Override
	public void cancelFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
		List<Pair<String, String>> params = new ArrayList<Pair<String,String>>();
		params.add(new Pair<String, String>(INFO_EXEC_ID, String.valueOf(exFlow.getExecutionId())));
		Map<String, Object> response;
		try {
			response = callRemoteExecutorManager(ACTION_CANCEL_FLOW, userId, params);
			if(response.containsKey(INFO_ERROR)) {
				throw new ExecutorManagerException((String)response.get(INFO_ERROR));
			}
		} catch (Exception e) {
			throw new ExecutorManagerException(e);
		}	
	}
	
	@Override
	public void resumeFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
		List<Pair<String, String>> params = new ArrayList<Pair<String,String>>();
		params.add(new Pair<String, String>(INFO_EXEC_ID, String.valueOf(exFlow.getExecutionId())));
		Map<String, Object> response;
		try {
			response = callRemoteExecutorManager(ACTION_RESUME_FLOW, userId, params);
			if(response.containsKey(INFO_ERROR)) {
				throw new ExecutorManagerException((String)response.get(INFO_ERROR));
			}
		} catch (Exception e) {
			throw new ExecutorManagerException(e);
		}	
	}
	
	@Override
	public void pauseFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
		List<Pair<String, String>> params = new ArrayList<Pair<String,String>>();
		params.add(new Pair<String, String>(INFO_EXEC_ID, String.valueOf(exFlow.getExecutionId())));
		Map<String, Object> response;
		try {
			response = callRemoteExecutorManager(ACTION_PAUSE_FLOW, userId, params);
			if(response.containsKey(INFO_ERROR)) {
				throw new ExecutorManagerException((String)response.get(INFO_ERROR));
			}
		} catch (Exception e) {
			throw new ExecutorManagerException(e);
		}	
	}
	
	@Override
	public void pauseExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_PAUSE_JOBS, userId, jobIds);
	}
	
	@Override
	public void resumeExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RESUME_JOBS, userId, jobIds);
	}
	
	@Override
	public void retryFailures(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_FAILURES, userId);
	}
	
	@Override
	public void retryExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_JOBS, userId, jobIds);
	}
	
	@Override
	public void disableExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_DISABLE_JOBS, userId, jobIds);
	}
	
	@Override
	public void enableExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_ENABLE_JOBS, userId, jobIds);
	}
	
	@Override
	public void cancelExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_CANCEL_JOBS, userId, jobIds);
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> modifyExecutingJobs(ExecutableFlow exFlow, String command, String userId, String ... jobIds) throws ExecutorManagerException {
		synchronized(exFlow) {
			Pair<ExecutionReference, ExecutableFlow> pair = runningFlows.get(exFlow.getExecutionId());
			if (pair == null) {
				throw new ExecutorManagerException("Execution " + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId() + " isn't running.");
			}
			
			Map<String, Object> response = null;
			if (jobIds != null && jobIds.length > 0) {
				for (String jobId: jobIds) {
					if (!jobId.isEmpty()) {
						ExecutableNode node = exFlow.getExecutableNode(jobId);
						if (node == null) {
							throw new ExecutorManagerException("Job " + jobId + " doesn't exist in execution " + exFlow.getExecutionId() + ".");
						}
					}
				}
				String ids = StringUtils.join(jobIds, ',');
				response = callExecutorServer(
						pair.getFirst(), 
						ConnectorParams.MODIFY_EXECUTION_ACTION, 
						userId, 
						new Pair<String,String>(ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE, command), 
						new Pair<String,String>(ConnectorParams.MODIFY_JOBS_LIST, ids));
			}
			else {
				response = callExecutorServer(
						pair.getFirst(), 
						ConnectorParams.MODIFY_EXECUTION_ACTION, 
						userId, 
						new Pair<String,String>(ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE, command));
			}
			
			return response;
		}
	}
	
	@Override
	public String submitExecutableFlow(ExecutableFlow exflow, String userId) throws ExecutorManagerException {
		List<Pair<String, String>> params = new ArrayList<Pair<String,String>>();
		params.add(new Pair<String, String>(INFO_EXEC_FLOW_JSON, JSONUtils.toJSON(exflow.toObject())));
		Map<String, Object> response;
		try {
			response = callRemoteExecutorManager(ACTION_SUBMIT_FLOW, userId, params);
			if(response.containsKey(INFO_ERROR)) {
				throw new ExecutorManagerException((String)response.get(INFO_ERROR));
			}
			String message = (String) response.get(INFO_MESSAGE);
			return message;
		} catch (Exception e) {
			throw new ExecutorManagerException(e);
		}	
	}
	
	private Map<String, Object> callExecutorServer(ExecutionReference ref, String action, Pair<String,String> ... params) throws ExecutorManagerException {
		try {
			return callExecutorServer(ref.getHost(), ref.getPort(), action, ref.getExecId(), null, params);
		} catch (IOException e) {
			throw new ExecutorManagerException(e);
		}
	}
	
	private Map<String, Object> callExecutorServer(ExecutionReference ref, String action, String user, Pair<String,String> ... params) throws ExecutorManagerException {
		try {
			return callExecutorServer(ref.getHost(), ref.getPort(), action, ref.getExecId(), user, params);
		} catch (IOException e) {
			throw new ExecutorManagerException(e);
		}
	}
	
	private Map<String, Object> callExecutorServer(String host, int port, String action, Integer executionId, String user, Pair<String,String> ... params) throws IOException {
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(host)
			.setPort(port)
			.setPath("/executor");

		builder.setParameter(ConnectorParams.ACTION_PARAM, action);
		
		if (executionId != null) {
			builder.setParameter(ConnectorParams.EXECID_PARAM,String.valueOf(executionId));
		}
		
		if (user != null) {
			builder.setParameter(ConnectorParams.USER_PARAM, user);
		}
		
		if (params != null) {
			for (Pair<String, String> pair: params) {
				builder.setParameter(pair.getFirst(), pair.getSecond());
			}
		}

		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			throw e;
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}
		
		@SuppressWarnings("unchecked")
		Map<String, Object> jsonResponse = (Map<String, Object>)JSONUtils.parseJSONFromString(response);
		String error = (String)jsonResponse.get(ConnectorParams.RESPONSE_ERROR);
		if (error != null) {
			throw new IOException(error);
		}
		
		return jsonResponse;
	}
	
	@Override
	public Map<String, Object> callExecutorJMX(String hostPort, String action, String mBean) throws IOException {
		URIBuilder builder = new URIBuilder();
		
		String[] hostPortSplit = hostPort.split(":");
		builder.setScheme("http")
			.setHost(hostPortSplit[0])
			.setPort(Integer.parseInt(hostPortSplit[1]))
			.setPath("/jmx");

		builder.setParameter(action, "");
		if (mBean != null) {
			builder.setParameter(ConnectorParams.JMX_MBEAN, mBean);
		}

		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			throw e;
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}
		
		@SuppressWarnings("unchecked")
		Map<String, Object> jsonResponse = (Map<String, Object>)JSONUtils.parseJSONFromString(response);
		String error = (String)jsonResponse.get(ConnectorParams.RESPONSE_ERROR);
		if (error != null) {
			throw new IOException(error);
		}
		return jsonResponse;
	}
	
	private Map<String, Object> callRemoteExecutorManager(String action, String user, List<Pair<String,String>> params) throws IOException {
		URIBuilder builder = new URIBuilder();
		builder.setScheme("http")
			.setHost(executorManagerHost)
			.setPort(executorManagerPort)
			.setPath(ExecutorManagerServlet.URL);

		builder.setParameter(ConnectorParams.ACTION_PARAM, action);
		
		if (user != null) {
			builder.setParameter(ConnectorParams.USER_PARAM, user);
		}
		
		if (params != null) {
			for (Pair<String, String> pair: params) {
				builder.setParameter(pair.getFirst(), pair.getSecond());
			}
		}

		URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		
		ResponseHandler<String> responseHandler = new BasicResponseHandler();
		
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpget = new HttpGet(uri);
		String response = null;
		try {
			response = httpclient.execute(httpget, responseHandler);
		} catch (IOException e) {
			throw e;
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}
		
		@SuppressWarnings("unchecked")
		Map<String, Object> jsonResponse = (Map<String, Object>)JSONUtils.parseJSONFromString(response);
		String error = (String)jsonResponse.get(ConnectorParams.RESPONSE_ERROR);
		if (error != null) {
			throw new IOException(error);
		}
		
		return jsonResponse;
	}
	
	@Override
	public void shutdown() {
		updater.shutdown();
	}
	
	private class UpdaterThread extends Thread {
		private boolean shutdown = false;

		public UpdaterThread() {
			this.setName("ExecutorManagerRemoteUpdaterThread");
		}
		
		// 10 mins recently finished threshold.
		private long recentlyFinishedLifetimeMs = 600000;
		private int waitTimeIdleMs = 2000;
		private int waitTimeMs = 500;
		
		private void shutdown() {
			shutdown = true;
		}
		
		@SuppressWarnings("unchecked")
		public void run() {
			while(!shutdown) {
				try {
					lastThreadCheckTime = System.currentTimeMillis();
					
//					loadRunningFlows();
					List<Pair<String, String>> params = new ArrayList<Pair<String,String>>();
					ArrayList<ExecutableFlow> finishedFlows = new ArrayList<ExecutableFlow>();
					
					List<Long> updateTimesList = new ArrayList<Long>();
					List<Integer> executionIdsList = new ArrayList<Integer>();
				
					// We pack the parameters of the same host together before we query.
					fillUpdateTimeAndExecId(executionIdsList, updateTimesList);
					
					params.add(new Pair<String, String>(INFO_UPDATE_TIME_LIST, JSONUtils.toJSON(updateTimesList)));
					params.add(new Pair<String, String>(INFO_EXEC_ID_LIST, JSONUtils.toJSON(executionIdsList)));
					
					Map<String, Object> results = null;
					try {
						results = callRemoteExecutorManager(ACTION_UPDATE, "azkaban", params);
					} catch (IOException e) {
						logger.error(e);
					}
					
					// We gets results
					if (results != null) {
						List<Map<String,Object>> executionUpdates = (List<Map<String,Object>>)results.get(INFO_UPDATES);
						for (Map<String,Object> updateMap: executionUpdates) {
							try {
								ExecutableFlow flow = updateExecution(updateMap);
								if (isFinished(flow)) {
									finishedFlows.add(flow);
								}
							} catch (ExecutorManagerException e) {
								logger.error(e);
							}
						}

						evictOldRecentlyFinished(recentlyFinishedLifetimeMs);
						// Add new finished
						for (ExecutableFlow flow: finishedFlows) {
							if(flow.getScheduleId() >= 0 && flow.getStatus() == Status.SUCCEEDED){
								ScheduleStatisticManager.invalidateCache(flow.getScheduleId());
							}
							recentlyFinished.put(flow.getExecutionId(), flow);
						}
						

					}
					
					synchronized(this) {
						try {
							if (runningFlows.size() > 0) {
								this.wait(waitTimeMs);
							}
							else {
								this.wait(waitTimeIdleMs);
							}
						} catch (InterruptedException e) {
						}
					}
				}
				catch (Exception e) {
					logger.error(e);
				}
			}
		}
	}
	
	private void evictOldRecentlyFinished(long ageMs) {
		ArrayList<Integer> recentlyFinishedKeys = new ArrayList<Integer>(recentlyFinished.keySet());
		long oldAgeThreshold = System.currentTimeMillis() - ageMs;
		for (Integer key: recentlyFinishedKeys) {
			ExecutableFlow flow = recentlyFinished.get(key);
			
			if (flow.getEndTime() < oldAgeThreshold) {
				// Evict
				recentlyFinished.remove(key);
			}
		}
	}
	
	private ExecutableFlow updateExecution(Map<String,Object> updateData) throws ExecutorManagerException {
		
		Integer execId = (Integer)updateData.get(ConnectorParams.UPDATE_MAP_EXEC_ID);
		if (execId == null) {
			throw new ExecutorManagerException("Response is malformed. Need exec id to update.");
		}
		
		Pair<ExecutionReference, ExecutableFlow> refPair = this.runningFlows.get(execId);
		if (refPair == null) {
			throw new ExecutorManagerException("No running flow found with the execution id.");
		}
		
		ExecutionReference ref = refPair.getFirst();
		ExecutableFlow flow = refPair.getSecond();
		if (updateData.containsKey("error")) {
			// The flow should be finished here.
			throw new ExecutorManagerException((String)updateData.get("error"), flow);
		}
		
		// Reset errors.
		ref.setNextCheckTime(0);
		ref.setNumErrors(0);
		flow.applyUpdateObject(updateData);
	
		return flow;
	}
	
	public boolean isFinished(ExecutableFlow flow) {
		switch(flow.getStatus()) {
		case SUCCEEDED:
		case FAILED:
		case KILLED:
			return true;
		default:
			return false;
		}
	}
	
	private void fillUpdateTimeAndExecId(List<Integer> executionIds, List<Long> updateTimes) {
		for (Pair<ExecutionReference, ExecutableFlow> flow: runningFlows.values()) {
			executionIds.add(flow.getSecond().getExecutionId());
			updateTimes.add(flow.getSecond().getUpdateTime());
		}
	}
	
	@Override
	public int getExecutableFlows(int projectId, String flowId, int from, int length, List<ExecutableFlow> outputList) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(projectId, flowId, from, length);
		outputList.addAll(flows);
		return executorLoader.fetchNumExecutableFlows(projectId, flowId);
	}

	@Override
	public List<ExecutableFlow> getExecutableFlows(int projectId, String flowId, int from, int length, Status status) throws ExecutorManagerException {
		return executorLoader.fetchFlowHistory(projectId, flowId, from, length, status);
	}


}
