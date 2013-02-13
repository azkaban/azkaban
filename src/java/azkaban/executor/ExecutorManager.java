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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import azkaban.executor.ExecutableFlow.Status;
import azkaban.project.Project;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;

/**
 * Executor manager used to manage the client side job.
 *
 */
public class ExecutorManager {
	private static Logger logger = Logger.getLogger(ExecutorManager.class);
	private ExecutorLoader executorLoader;
	private String executorHost;
	private int executorPort;
	
	private ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>> runningFlows = new ConcurrentHashMap<Integer, Pair<ExecutionReference, ExecutableFlow>>();
	private ConcurrentHashMap<Integer, ExecutableFlow> recentlyFinished = new ConcurrentHashMap<Integer, ExecutableFlow>();
	
	private ExecutorMailer mailer;
	private ExecutingManagerUpdaterThread executingManager;
	
	public ExecutorManager(Props props, ExecutorLoader loader) throws ExecutorManagerException {
		this.executorLoader = loader;
		this.loadRunningFlows();
		
		executorHost = props.getString("executor.host", "localhost");
		executorPort = props.getInt("executor.port");
		mailer = new ExecutorMailer(props);
		executingManager = new ExecutingManagerUpdaterThread();
		executingManager.start();
	}
	
	private void loadRunningFlows() throws ExecutorManagerException {
		runningFlows.putAll(executorLoader.fetchActiveFlows());
	}
	
	public List<Integer> getRunningFlows(int projectId, String flowId) {
		ArrayList<Integer> executionIds = new ArrayList<Integer>();
		for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
			if (ref.getSecond().getFlowId().equals(flowId)) {
				executionIds.add(ref.getFirst().getExecId());
			}
		}
		
		return executionIds;
	}
	
	public boolean isFlowRunning(int projectId, String flowId) {
		for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
			if (ref.getSecond().getFlowId().equals(flowId)) {
				return true;
			}
		}
		
		return false;
	}
	
	public ExecutableFlow getExecutableFlow(int execId) throws ExecutorManagerException {
		Pair<ExecutionReference, ExecutableFlow> active = runningFlows.get(execId);
		
		if (active == null) {
			return executorLoader.fetchExecutableFlow(execId);
		}

		return active.getSecond();
	}
	
	public List<ExecutableFlow> getRunningFlows() {
		ArrayList<ExecutableFlow> flows = new ArrayList<ExecutableFlow>();
		for (Pair<ExecutionReference, ExecutableFlow> ref : runningFlows.values()) {
			flows.add(ref.getSecond());
		}
		return flows;
	}
	
	public List<ExecutableFlow> getRecentlyFinishedFlows() {
		return new ArrayList<ExecutableFlow>(recentlyFinished.values());
	}
	
	public List<ExecutableFlow> getExecutableFlows(Project project, String flowId, int skip, int size) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(project.getId(), flowId, skip, size);
		return flows;
	}
	
	public List<ExecutableFlow> getExecutableFlows(int skip, int size) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(skip, size);
		return flows;
	}
	
	public List<ExecutableFlow> getExecutableFlows(String flowIdContains, int skip, int size) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(null, '%'+flowIdContains+'%', null, 0, -1, -1 , skip, size);
		return flows;
	}

	public List<ExecutableFlow> getExecutableFlows(String projContain, String flowContain, String userContain, int status, long begin, long end, int skip, int size) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(projContain, flowContain, userContain, status, begin, end , skip, size);
		return flows;
	}
	
	public List<ExecutableJobInfo> getExecutableJobs(Project project, String jobId, int skip, int size) throws ExecutorManagerException {
		List<ExecutableJobInfo> nodes = executorLoader.fetchJobHistory(project.getId(), jobId, skip, size);
		return nodes;
	}
	
	public int getNumberOfJobExecutions(Project project, String jobId) throws ExecutorManagerException{
		return executorLoader.fetchNumExecutableNodes(project.getId(), jobId);
	}
	
	public int getNumberOfExecutions(Project project, String flowId) throws ExecutorManagerException{
		return executorLoader.fetchNumExecutableFlows(project.getId(), flowId);
	}
	
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
	
	public void cancelFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
		synchronized(exFlow) {
			Pair<ExecutionReference, ExecutableFlow> pair = runningFlows.get(exFlow.getExecutionId());
			if (pair == null) {
				throw new ExecutorManagerException("Execution " + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId() + " isn't running.");
			}
			callExecutorServer(pair.getFirst(), ConnectorParams.CANCEL_ACTION, userId);
		}
	}
	
	public void resumeFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
		synchronized(exFlow) {
			Pair<ExecutionReference, ExecutableFlow> pair = runningFlows.get(exFlow.getExecutionId());
			if (pair == null) {
				throw new ExecutorManagerException("Execution " + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId() + " isn't running.");
			}
			callExecutorServer(pair.getFirst(), ConnectorParams.RESUME_ACTION, userId);
		}
	}
	
	public void pauseFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException {
		synchronized(exFlow) {
			Pair<ExecutionReference, ExecutableFlow> pair = runningFlows.get(exFlow.getExecutionId());
			if (pair == null) {
				throw new ExecutorManagerException("Execution " + exFlow.getExecutionId() + " of flow " + exFlow.getFlowId() + " isn't running.");
			}
			callExecutorServer(pair.getFirst(), ConnectorParams.PAUSE_ACTION, userId);
		}
	}
	
	public void pauseExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_PAUSE_JOBS, userId, jobIds);
	}
	
	public void resumeExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RESUME_JOBS, userId, jobIds);
	}
	
	public void retryExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_RETRY_JOBS, userId, jobIds);
	}
	
	public void disableExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_DISABLE_JOBS, userId, jobIds);
	}
	
	public void enableExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException {
		modifyExecutingJobs(exFlow, ConnectorParams.MODIFY_ENABLE_JOBS, userId, jobIds);
	}
	
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
			
			for (String jobId: jobIds) {
				if (!jobId.isEmpty()) {
					ExecutableNode node = exFlow.getExecutableNode(jobId);
					if (node == null) {
						throw new ExecutorManagerException("Job " + jobId + " doesn't exist in execution " + exFlow.getExecutionId() + ".");
					}
				}
			}
			String ids = StringUtils.join(jobIds, ',');
			Map<String, Object> response = callExecutorServer(
					pair.getFirst(), 
					ConnectorParams.MODIFY_EXECUTION_ACTION, 
					userId, 
					new Pair<String,String>(ConnectorParams.MODIFY_EXECUTION_ACTION_TYPE, command), 
					new Pair<String,String>(ConnectorParams.MODIFY_JOBS_LIST, ids));
			
			return response;
		}
	}
	
	public void submitExecutableFlow(ExecutableFlow exflow) throws ExecutorManagerException {
		synchronized(exflow) {
			logger.info("Submitting execution flow " + exflow.getFlowId());

			// The exflow id is set by the loader. So it's unavailable until after this call.
			executorLoader.uploadExecutableFlow(exflow);
			
			// We create an active flow reference in the datastore. If the upload fails, we remove the reference.
			ExecutionReference reference = new ExecutionReference(exflow.getExecutionId(), executorHost, executorPort);
			executorLoader.addActiveExecutableReference(reference);
			try {
				callExecutorServer(reference,  ConnectorParams.EXECUTE_ACTION);
				runningFlows.put(exflow.getExecutionId(), new Pair<ExecutionReference, ExecutableFlow>(reference, exflow));
			}
			catch (ExecutorManagerException e) {
				executorLoader.removeActiveExecutableReference(reference.getExecId());
				throw e;
			}
		}
	}

	private Map<String, Object> callExecutorServer(ExecutionReference ref, String action) throws ExecutorManagerException {
		try {
			return callExecutorServer(ref.getHost(), ref.getPort(), action, ref.getExecId(), null, (Pair<String,String>[])null);
		} catch (IOException e) {
			throw new ExecutorManagerException(e);
		}
	}
	
	private Map<String, Object> callExecutorServer(ExecutionReference ref, String action, String user) throws ExecutorManagerException {
		try {
			return callExecutorServer(ref.getHost(), ref.getPort(), action, ref.getExecId(), user, (Pair<String,String>[])null);
		} catch (IOException e) {
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
	
	private class ExecutingManagerUpdaterThread extends Thread {
		private boolean shutdown = false;

		public ExecutingManagerUpdaterThread() {
			this.setName("ExecutorManagerUpdaterThread");
		}
		
		// 10 mins recently finished threshold.
		private long recentlyFinishedLifetimeMs = 600000;
		private int waitTimeIdleMs = 2000;
		private int waitTimeMs = 500;
		
		// When we have an http error, for that flow, we'll check every 10 secs, 6 times (1 mins) before we evict.
		private int numErrors = 6;
		private long errorThreshold = 10000;
		
		@SuppressWarnings("unchecked")
		public void run() {
			while(!shutdown) {
				try {
					Map<ConnectionInfo, List<ExecutableFlow>> exFlowMap = getFlowToExecutorMap();
					ArrayList<ExecutableFlow> finishedFlows = new ArrayList<ExecutableFlow>();
					ArrayList<ExecutableFlow> finalizeFlows = new ArrayList<ExecutableFlow>();
					
					if (exFlowMap.size() > 0) {
						for (Map.Entry<ConnectionInfo, List<ExecutableFlow>> entry: exFlowMap.entrySet()) {
							List<Long> updateTimesList = new ArrayList<Long>();
							List<Integer> executionIdsList = new ArrayList<Integer>();
						
							// We pack the parameters of the same host together before we query.
							fillUpdateTimeAndExecId(entry.getValue(), executionIdsList, updateTimesList);
							
							Pair<String,String> updateTimes = new Pair<String, String>(
									ConnectorParams.UPDATE_TIME_LIST_PARAM, 
									JSONUtils.toJSON(updateTimesList));
							Pair<String,String> executionIds = new Pair<String, String>(
									ConnectorParams.EXEC_ID_LIST_PARAM, 
									JSONUtils.toJSON(executionIdsList));
							
							ConnectionInfo connection = entry.getKey();
							Map<String, Object> results = null;
							try {
								results = callExecutorServer(connection.getHost(), connection.getPort(), ConnectorParams.UPDATE_ACTION, null, null, executionIds, updateTimes);
							} catch (IOException e) {
								logger.error(e);
								for (ExecutableFlow flow: entry.getValue()) {
									Pair<ExecutionReference, ExecutableFlow> pair = runningFlows.get(flow.getExecutionId());
									if (pair != null) {
										ExecutionReference ref = pair.getFirst();
										int numErrors = ref.getNumErrors();
										if (ref.getNumErrors() < this.numErrors) {
											ref.setNextCheckTime(System.currentTimeMillis() + errorThreshold);
											ref.setNumErrors(++numErrors);
										}
										else {
											logger.error("Evicting flow " + flow.getExecutionId() + ". The executor is unresponsive.");
											//TODO should send out an unresponsive email here.
											finalizeFlows.add(pair.getSecond());
										}
									}
								}
							}
							
							// We gets results
							if (results != null) {
								List<Map<String,Object>> executionUpdates = (List<Map<String,Object>>)results.get(ConnectorParams.RESPONSE_UPDATED_FLOWS);
								for (Map<String,Object> updateMap: executionUpdates) {
									try {
										ExecutableFlow flow = updateExecution(updateMap);
										if (isFinished(flow)) {
											finishedFlows.add(flow);
											finalizeFlows.add(flow);
										}
									} catch (ExecutorManagerException e) {
										ExecutableFlow flow = e.getExecutableFlow();
										logger.error(e);

										if (flow != null) {
											logger.error("Finalizing flow " + flow.getExecutionId());
											finalizeFlows.add(flow);
										}
									}
								}
							}
						}
	
						evictOldRecentlyFinished(recentlyFinishedLifetimeMs);
						// Add new finished
						for (ExecutableFlow flow: finishedFlows) {
							recentlyFinished.put(flow.getExecutionId(), flow);
						}
						
						// Kill error flows
						for (ExecutableFlow flow: finalizeFlows) {
							finalizeFlows(flow);
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
	
	private void finalizeFlows(ExecutableFlow flow) {
		int execId = flow.getExecutionId();
		
		// First we check if the execution in the datastore is complete
		try {
			ExecutableFlow dsFlow;
			if(isFinished(flow)) {
				dsFlow = flow;
			}
			else {
				dsFlow = executorLoader.fetchExecutableFlow(execId);
			
				// If it's marked finished, we're good. If not, we fail everything and then mark it finished.
				if (!isFinished(dsFlow)) {
					failEverything(dsFlow);
					executorLoader.updateExecutableFlow(dsFlow);
				}
			}

			// Delete the executing reference.
			executorLoader.removeActiveExecutableReference(execId);
			
			runningFlows.remove(execId);
			recentlyFinished.put(execId, dsFlow);
		} catch (ExecutorManagerException e) {
			logger.error(e);
		}
		
		// TODO append to the flow log that we forced killed this flow because the target no longer had
		// the reference.
		
		// But we can definitely email them.
		if(flow.getStatus() == Status.FAILED || flow.getStatus() == Status.KILLED)
		{
			if(flow.getFailureEmails() != null && !flow.getFailureEmails().isEmpty())
			{
				try {
					mailer.sendErrorEmail(flow, "Executor no longer seems to be running this execution. Most likely due to executor bounce.");
				} catch (Exception e) {
					logger.error(e);
				}
			}
		}
		else
		{
			if(flow.getSuccessEmails() != null && !flow.getSuccessEmails().isEmpty())
			{
				try {
					mailer.sendSuccessEmail(flow);
				} catch (Exception e) {
					logger.error(e);
				}
			}
		}
		
	}
	
	private void failEverything(ExecutableFlow exFlow) {
		long time = System.currentTimeMillis();
		for (ExecutableNode node: exFlow.getExecutableNodes()) {
			switch(node.getStatus()) {
				case SUCCEEDED:
				case FAILED:
				case KILLED:
				case SKIPPED:
				case DISABLED:
					continue;
				//case UNKNOWN:
				case READY:
					node.setStatus(Status.KILLED);
					break;
				default:
					node.setStatus(Status.FAILED);
					break;
			}

			if (node.getStartTime() == -1) {
				node.setStartTime(time);
			}
			if (node.getEndTime() == -1) {
				node.setEndTime(time);
			}
		}

		if (exFlow.getEndTime() == -1) {
			exFlow.setEndTime(time);
		}
		
		exFlow.setStatus(Status.FAILED);
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
		Status oldStatus = flow.getStatus();
		
		flow.applyUpdateObject(updateData);
		Status newStatus = flow.getStatus();
		
		if (oldStatus != newStatus && newStatus.equals(Status.FAILED_FINISHING)) {
			// We want to see if we should give an email status on first failure.
			if (flow.getNotifyOnFirstFailure()) {
				mailer.sendFirstErrorMessage(flow);
			}
		}
		
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
	
	private void fillUpdateTimeAndExecId(List<ExecutableFlow> flows, List<Integer> executionIds, List<Long> updateTimes) {
		for (ExecutableFlow flow: flows) {
			executionIds.add(flow.getExecutionId());
			updateTimes.add(flow.getUpdateTime());
		}
	}
	
	private Map<ConnectionInfo, List<ExecutableFlow>> getFlowToExecutorMap() {
		HashMap<ConnectionInfo, List<ExecutableFlow>> exFlowMap = new HashMap<ConnectionInfo, List<ExecutableFlow>>();
		
		ConnectionInfo lastPort = new ConnectionInfo(executorHost, executorPort);
		for (Pair<ExecutionReference, ExecutableFlow> runningFlow: runningFlows.values()) {
			ExecutionReference ref = runningFlow.getFirst();
			ExecutableFlow flow = runningFlow.getSecond();
			
			// We can set the next check time to prevent the checking of certain flows.
			if (ref.getNextCheckTime() >= System.currentTimeMillis()) {
				continue;
			}
			
			// Just a silly way to reduce object creation construction of objects since it's most likely that the values will be the same.
			if (!lastPort.isEqual(ref.getHost(), ref.getPort())) {
				lastPort = new ConnectionInfo(ref.getHost(), ref.getPort());
			}
			
			List<ExecutableFlow> flows = exFlowMap.get(lastPort);
			if (flows == null) {
				flows = new ArrayList<ExecutableFlow>();
				exFlowMap.put(lastPort, flows);
			}
			
			flows.add(flow);
		}
		
		return exFlowMap;
	}
	
	private static class ConnectionInfo {
		private String host;
		private int port;

		public ConnectionInfo(String host, int port) {
			this.host = host;
			this.port = port;
		}

		@SuppressWarnings("unused")
		private ConnectionInfo getOuterType() {
			return ConnectionInfo.this;
		}
		
		public boolean isEqual(String host, int port) {
			return this.port == port && this.host.equals(host);
		}
		
		public String getHost() {
			return host;
		}
		
		public int getPort() {
			return port;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((host == null) ? 0 : host.hashCode());
			result = prime * result + port;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ConnectionInfo other = (ConnectionInfo) obj;
			if (host == null) {
				if (other.host != null)
					return false;
			} else if (!host.equals(other.host))
				return false;
			if (port != other.port)
				return false;
			return true;
		}
	}
	
	public int getExecutableFlows(int projectId, String flowId, int from, int length, List<ExecutableFlow> outputList) throws ExecutorManagerException {
		List<ExecutableFlow> flows = executorLoader.fetchFlowHistory(projectId, flowId, from, length);
		outputList.addAll(flows);
		return executorLoader.fetchNumExecutableFlows(projectId, flowId);
	}
}
