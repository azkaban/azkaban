package azkaban.executor;

import java.io.IOException;
import java.lang.Thread.State;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.project.Project;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;

public interface ExecutorManagerAdapter{
	
	public static final String LOCAL_MODE = "local";
	public static final String REMOTE_MODE = "remote";
	
	public static final String REMOTE_EXECUTOR_MANAGER_HOST = "remote.executor.manager.host";
	public static final String REMOTE_EXECUTOR_MANAGER_PORT = "remote.executor.manager.port";
	public static final String REMOTE_EXECUTOR_MANAGER_URL = "/executormanager";
	
	public static final String ACTION_GET_FLOW_LOG = "getFlowLog";
	public static final String ACTION_GET_JOB_LOG = "getJobLog";
	public static final String ACTION_CANCEL_FLOW = "cancelFlow";
	public static final String ACTION_SUBMIT_FLOW = "submitFlow";
	public static final String ACTION_RESUME_FLOW = "resumeFlow";
	public static final String ACTION_PAUSE_FLOW = "pauseFlow";
	public static final String ACTION_MODIFY_EXECUTION = "modifyExecution"; 
	public static final String ACTION_UPDATE = "update";
	public static final String ACTION_GET_JMX = "getJMX";
	
	public static final String COMMAND_MODIFY_PAUSE_JOBS = "modifyPauseJobs";
	public static final String COMMAND_MODIFY_RESUME_JOBS = "modifyResumeJobs";
	public static final String COMMAND_MODIFY_RETRY_FAILURES = "modifyRetryFailures";
	public static final String COMMAND_MODIFY_RETRY_JOBS = "modifyRetryJobs";
	public static final String COMMAND_MODIFY_DISABLE_JOBS = "modifyDisableJobs";
	public static final String COMMAND_MODIFY_ENABLE_JOBS = "modifyEnableJobs";
	public static final String COMMAND_MODIFY_CANCEL_JOBS = "modifyCancelJobs";
	
	public static final String INFO_JMX_TYPE = "jmxType";
	public static final String INFO_JMX_DATA = "jmxData";
	public static final String INFO_ACTION = "action";
	public static final String INFO_TYPE = "type";
	public static final String INFO_EXEC_ID = "execId";
	public static final String INFO_EXEC_FLOW_JSON = "execFlowJson";
	public static final String INFO_PROJECT_ID = "projectId";
	public static final String INFO_FLOW_NAME = "flowName";
	public static final String INFO_JOB_NAME = "jobName";
	public static final String INFO_OFFSET = "offset";
	public static final String INFO_LENGTH = "length";
	public static final String INFO_ATTEMPT = "attempt";
	public static final String INFO_MODIFY_JOB_IDS = "modifyJobIds";
	public static final String INFO_MODIFY_COMMAND = "modifyCommand";
	public static final String INFO_MESSAGE = "message";
	public static final String INFO_ERROR = "error";
	public static final String INFO_UPDATE_TIME_LIST = "updateTimeList";
	public static final String INFO_EXEC_ID_LIST = "execIdList";
	public static final String INFO_UPDATES = "updates";
	public static final String INFO_USER_ID = "userId";
	public static final String INFO_LOG = "logData";
	
	public boolean isFlowRunning(int projectId, String flowId);
	
	public ExecutableFlow getExecutableFlow(int execId) throws ExecutorManagerException;
	
	public List<Integer> getRunningFlows(int projectId, String flowId);
	
	public List<ExecutableFlow> getRunningFlows() throws IOException;
	
	public List<ExecutableFlow> getRecentlyFinishedFlows();
	
	public List<ExecutableFlow> getExecutableFlows(Project project, String flowId, int skip, int size) throws ExecutorManagerException;
	
	public List<ExecutableFlow> getExecutableFlows(int skip, int size) throws ExecutorManagerException;
	
	public List<ExecutableFlow> getExecutableFlows(String flowIdContains, int skip, int size) throws ExecutorManagerException;
	
	public List<ExecutableFlow> getExecutableFlows(String projContain, String flowContain, String userContain, int status, long begin, long end, int skip, int size) throws ExecutorManagerException;

	public int getExecutableFlows(int projectId, String flowId, int from, int length, List<ExecutableFlow> outputList) throws ExecutorManagerException;

	public List<ExecutableFlow> getExecutableFlows(int projectId, String flowId, int from, int length, Status status) throws ExecutorManagerException;

	public List<ExecutableJobInfo> getExecutableJobs(Project project, String jobId, int skip, int size) throws ExecutorManagerException;
	
	public int getNumberOfJobExecutions(Project project, String jobId) throws ExecutorManagerException;
	
	public int getNumberOfExecutions(Project project, String flowId) throws ExecutorManagerException;
	
	public LogData getExecutableFlowLog(ExecutableFlow exFlow, int offset, int length) throws ExecutorManagerException;
	
	public LogData getExecutionJobLog(ExecutableFlow exFlow, String jobId, int offset, int length, int attempt) throws ExecutorManagerException;

	public List<Object> getExecutionJobStats(ExecutableFlow exflow, String jobId, int attempt) throws ExecutorManagerException;
	
	public JobMetaData getExecutionJobMetaData(ExecutableFlow exFlow, String jobId, int offset, int length, int attempt) throws ExecutorManagerException;
	
	public void cancelFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException;
	
	public void resumeFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException;
	
	public void pauseFlow(ExecutableFlow exFlow, String userId) throws ExecutorManagerException;
	
	public void pauseExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException;
	
	public void resumeExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException;
	
	public void retryFailures(ExecutableFlow exFlow, String userId) throws ExecutorManagerException;
	
	public void retryExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException;
	
	public void disableExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException;
	
	public void enableExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException;
	
	public void cancelExecutingJobs(ExecutableFlow exFlow, String userId, String ... jobIds) throws ExecutorManagerException;

	public String submitExecutableFlow(ExecutableFlow exflow, String userId) throws ExecutorManagerException;
	
	public Map<String, Object> callExecutorJMX(String hostPort, String action, String mBean) throws IOException;

	public void shutdown();

	public Set<String> getAllActiveExecutorServerHosts();

	public State getExecutorManagerThreadState();

	public boolean isExecutorManagerThreadActive();

	public long getLastExecutorManagerThreadCheckTime();

	public Set<? extends String> getPrimaryServerHosts();
	
}
