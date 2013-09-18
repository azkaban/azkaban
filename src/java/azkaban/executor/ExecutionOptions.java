package azkaban.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Execution options for submitted flows and scheduled flows
 */
public class ExecutionOptions {
	public static final String CONCURRENT_OPTION_SKIP="skip";
	public static final String CONCURRENT_OPTION_PIPELINE="pipeline";
	public static final String CONCURRENT_OPTION_IGNORE="ignore";
	
	private static final String FLOW_PARAMETERS = "flowParameters";
	private static final String NOTIFY_ON_FIRST_FAILURE = "notifyOnFirstFailure";
	private static final String NOTIFY_ON_LAST_FAILURE = "notifyOnLastFailure";
	private static final String SUCCESS_EMAILS = "successEmails";
	private static final String FAILURE_EMAILS = "failureEmails";
	private static final String FAILURE_ACTION = "failureAction";
	private static final String PIPELINE_LEVEL = "pipelineLevel";
	private static final String PIPELINE_EXECID = "pipelineExecId";
	private static final String QUEUE_LEVEL = "queueLevel";
	private static final String CONCURRENT_OPTION = "concurrentOption";
	private static final String DISABLE = "disabled";
	private static final String FAILURE_EMAILS_OVERRIDE = "failureEmailsOverride";
	private static final String SUCCESS_EMAILS_OVERRIDE = "successEmailsOverride";
	
	private boolean notifyOnFirstFailure = true;
	private boolean notifyOnLastFailure = false;
	private boolean failureEmailsOverride = false;
	private boolean successEmailsOverride = false;
	private ArrayList<String> failureEmails = new ArrayList<String>();
	private ArrayList<String> successEmails = new ArrayList<String>();
	
	private Integer pipelineLevel = null;
	private Integer pipelineExecId = null;
	private Integer queueLevel = 0;
	private String concurrentOption = CONCURRENT_OPTION_IGNORE;
	private Map<String, String> flowParameters = new HashMap<String, String>();
	
	public enum FailureAction {
		FINISH_CURRENTLY_RUNNING,
		CANCEL_ALL,
		FINISH_ALL_POSSIBLE
	}
	
	private FailureAction failureAction = FailureAction.FINISH_CURRENTLY_RUNNING;
	
	private Set<String> initiallyDisabledJobs = new HashSet<String>();
	
	public void addAllFlowParameters(Map<String,String> flowParam) {
		flowParameters.putAll(flowParam);
	}
	
	public Map<String,String> getFlowParameters() {
		return flowParameters;
	}
	
	public void setFailureEmails(Collection<String> emails) {
		failureEmails = new ArrayList<String>(emails);
	}
	
	public boolean isFailureEmailsOverridden() {
		return this.failureEmailsOverride;
	}
	
	public boolean isSuccessEmailsOverridden() {
		return this.successEmailsOverride;
	}

	public void setSuccessEmailsOverridden(boolean override) {
		this.successEmailsOverride = override;
	}
	
	public void setFailureEmailsOverridden(boolean override) {
		this.failureEmailsOverride = override;
	}
	
	public List<String> getFailureEmails() {
		return failureEmails;
	}
	
	public void setSuccessEmails(Collection<String> emails) {
		successEmails = new ArrayList<String>(emails);
	}
	
	public List<String> getSuccessEmails() {
		return successEmails;
	}
	
	public boolean getNotifyOnFirstFailure() {
		return notifyOnFirstFailure;
	}
	
	public boolean getNotifyOnLastFailure() {
		return notifyOnLastFailure;
	}
	
	public void setNotifyOnFirstFailure(boolean notify) {
		this.notifyOnFirstFailure = notify;
	}
	
	public void setNotifyOnLastFailure(boolean notify) {
		this.notifyOnLastFailure = notify;
	}
	
	public FailureAction getFailureAction() {
		return failureAction;
	}
	
	public void setFailureAction(FailureAction action) {
		failureAction = action;
	}
	
	public void setConcurrentOption(String concurrentOption) {
		this.concurrentOption = concurrentOption;
	}
	
	public String getConcurrentOption() {
		return concurrentOption;
	}
	
	public Integer getPipelineLevel() {
		return pipelineLevel;
	}
	
	public Integer getPipelineExecutionId() {
		return pipelineExecId;
	}
	
	public void setPipelineLevel(Integer level) {
		pipelineLevel = level;
	}
	
	public void setPipelineExecutionId(Integer id) {
		this.pipelineExecId = id;
	}
	
	public Integer getQueueLevel() {
		return queueLevel;
	}
	
	public List<String> getDisabledJobs() {
		return new ArrayList<String>(initiallyDisabledJobs);
	}
	
	public void setDisabledJobs(List<String> disabledJobs) {
		initiallyDisabledJobs = new HashSet<String>(disabledJobs);
	}
	
	public Map<String,Object> toObject() {
		HashMap<String,Object> flowOptionObj = new HashMap<String,Object>();
		
		flowOptionObj.put(FLOW_PARAMETERS, this.flowParameters);
		flowOptionObj.put(NOTIFY_ON_FIRST_FAILURE, this.notifyOnFirstFailure);
		flowOptionObj.put(NOTIFY_ON_LAST_FAILURE, this.notifyOnLastFailure);
		flowOptionObj.put(SUCCESS_EMAILS, successEmails);
		flowOptionObj.put(FAILURE_EMAILS, failureEmails);
		flowOptionObj.put(FAILURE_ACTION, failureAction.toString());
		flowOptionObj.put(PIPELINE_LEVEL, pipelineLevel);
		flowOptionObj.put(PIPELINE_EXECID, pipelineExecId);
		flowOptionObj.put(QUEUE_LEVEL, queueLevel);
		flowOptionObj.put(CONCURRENT_OPTION, concurrentOption);
		flowOptionObj.put(DISABLE, initiallyDisabledJobs);
		flowOptionObj.put(FAILURE_EMAILS_OVERRIDE, failureEmailsOverride);
		flowOptionObj.put(SUCCESS_EMAILS_OVERRIDE, successEmailsOverride);
		return flowOptionObj;
	}
	
	@SuppressWarnings("unchecked")
	public static ExecutionOptions createFromObject(Object obj) {
		if (obj == null || !(obj instanceof Map)) {
			return null;
		}
		
		Map<String,Object> optionsMap = (Map<String,Object>)obj;
		
		ExecutionOptions options = new ExecutionOptions();
		if (optionsMap.containsKey(FLOW_PARAMETERS)) {
			options.flowParameters = new HashMap<String, String>();
			options.flowParameters.putAll((Map<String,String>)optionsMap.get(FLOW_PARAMETERS));
		}
		// Failure notification
		if (optionsMap.containsKey(NOTIFY_ON_FIRST_FAILURE)) {
			options.notifyOnFirstFailure = (Boolean)optionsMap.get(NOTIFY_ON_FIRST_FAILURE);
		}
		if (optionsMap.containsKey(NOTIFY_ON_LAST_FAILURE)) {
			options.notifyOnLastFailure = (Boolean)optionsMap.get(NOTIFY_ON_LAST_FAILURE);
		}
		if (optionsMap.containsKey(CONCURRENT_OPTION)) {
			options.concurrentOption = (String)optionsMap.get(CONCURRENT_OPTION);
		}
		if (optionsMap.containsKey(DISABLE)) {
			options.initiallyDisabledJobs = new HashSet<String>((Collection<String>)optionsMap.get(DISABLE));
		}
		
		// Failure action
		if (optionsMap.containsKey(FAILURE_ACTION)) {
			options.failureAction = FailureAction.valueOf((String)optionsMap.get(FAILURE_ACTION));
		}
		options.pipelineLevel = (Integer)optionsMap.get(PIPELINE_LEVEL);
		options.pipelineExecId = (Integer)optionsMap.get(PIPELINE_EXECID);
		options.queueLevel = (Integer)optionsMap.get(QUEUE_LEVEL);
		
		// Success emails
		if (optionsMap.containsKey(SUCCESS_EMAILS)) {
			options.setSuccessEmails((List<String>)optionsMap.get(SUCCESS_EMAILS));
		}
		// Failure emails
		if (optionsMap.containsKey(FAILURE_EMAILS)) {
			options.setFailureEmails((List<String>)optionsMap.get(FAILURE_EMAILS));
		}
		
		if (optionsMap.containsKey(SUCCESS_EMAILS_OVERRIDE)) {
			options.setSuccessEmailsOverridden((Boolean)optionsMap.get(SUCCESS_EMAILS_OVERRIDE));
		}
		
		if (optionsMap.containsKey(FAILURE_EMAILS_OVERRIDE)) {
			options.setFailureEmailsOverridden((Boolean)optionsMap.get(FAILURE_EMAILS_OVERRIDE));
		}
		
		return options;
	}
}
