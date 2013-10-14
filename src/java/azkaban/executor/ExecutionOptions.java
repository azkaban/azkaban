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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.executor.mail.DefaultMailCreator;

/**
 * Execution options for submitted flows and scheduled flows
 */
public class ExecutionOptions {
	public static final String CONCURRENT_OPTION_SKIP="skip";
	public static final String CONCURRENT_OPTION_PIPELINE="pipeline";
	public static final String CONCURRENT_OPTION_IGNORE="ignore";
	
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
	private String mailCreator = DefaultMailCreator.DEFAULT_MAIL_CREATOR;
	private Map<String, String> flowParameters = new HashMap<String, String>();
	
	public enum FailureAction {
		FINISH_CURRENTLY_RUNNING,
		CANCEL_ALL,
		FINISH_ALL_POSSIBLE
	}
	
	private FailureAction failureAction = FailureAction.FINISH_CURRENTLY_RUNNING;
	
	private Set<String> initiallyDisabledJobs = new HashSet<String>();
	
	public void setFlowParameters(Map<String,String> flowParam) {
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
	
	public void setMailCreator(String mailCreator) {
		this.mailCreator = mailCreator;
	}
	
	public String getConcurrentOption() {
		return concurrentOption;
	}
	
	public String getMailCreator() {
		return mailCreator;
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
		
		flowOptionObj.put("flowParameters", this.flowParameters);
		flowOptionObj.put("notifyOnFirstFailure", this.notifyOnFirstFailure);
		flowOptionObj.put("notifyOnLastFailure", this.notifyOnLastFailure);
		flowOptionObj.put("successEmails", successEmails);
		flowOptionObj.put("failureEmails", failureEmails);
		flowOptionObj.put("failureAction", failureAction.toString());
		flowOptionObj.put("pipelineLevel", pipelineLevel);
		flowOptionObj.put("pipelineExecId", pipelineExecId);
		flowOptionObj.put("queueLevel", queueLevel);
		flowOptionObj.put("concurrentOption", concurrentOption);
		flowOptionObj.put("mailCreator", mailCreator);
		flowOptionObj.put("disabled", initiallyDisabledJobs);
		flowOptionObj.put("failureEmailsOverride", failureEmailsOverride);
		flowOptionObj.put("successEmailsOverride", successEmailsOverride);
		return flowOptionObj;
	}
	
	@SuppressWarnings("unchecked")
	public static ExecutionOptions createFromObject(Object obj) {
		if (obj == null || !(obj instanceof Map)) {
			return null;
		}
		
		Map<String,Object> optionsMap = (Map<String,Object>)obj;
		
		ExecutionOptions options = new ExecutionOptions();
		if (optionsMap.containsKey("flowParameters")) {
			options.flowParameters = new HashMap<String, String>((Map<String,String>)optionsMap.get("flowParameters"));
		}
		// Failure notification
		if (optionsMap.containsKey("notifyOnFirstFailure")) {
			options.notifyOnFirstFailure = (Boolean)optionsMap.get("notifyOnFirstFailure");
		}
		if (optionsMap.containsKey("notifyOnLastFailure")) {
			options.notifyOnLastFailure = (Boolean)optionsMap.get("notifyOnLastFailure");
		}
		if (optionsMap.containsKey("concurrentOption")) {
			options.concurrentOption = (String)optionsMap.get("concurrentOption");
		}
		if (optionsMap.containsKey("mailCreator")) {
			options.mailCreator = (String)optionsMap.get("mailCreator");
		}
		if (optionsMap.containsKey("disabled")) {
			options.initiallyDisabledJobs = new HashSet<String>((List<String>)optionsMap.get("disabled"));
		}
		
		// Failure action
		if (optionsMap.containsKey("failureAction")) {
			options.failureAction = FailureAction.valueOf((String)optionsMap.get("failureAction"));
		}
		options.pipelineLevel = (Integer)optionsMap.get("pipelineLevel");
		options.pipelineExecId = (Integer)optionsMap.get("pipelineExecId");
		options.queueLevel = (Integer)optionsMap.get("queueLevel");
		
		// Success emails
		if (optionsMap.containsKey("successEmails")) {
			options.setSuccessEmails((List<String>)optionsMap.get("successEmails"));
		}
		// Failure emails
		if (optionsMap.containsKey("failureEmails")) {
			options.setFailureEmails((List<String>)optionsMap.get("failureEmails"));
		}
		
		if (optionsMap.containsKey("successEmailsOverride")) {
			options.setSuccessEmailsOverridden((Boolean)optionsMap.get("successEmailsOverride"));
		}
		
		if (optionsMap.containsKey("failureEmailsOverride")) {
			options.setFailureEmailsOverridden((Boolean)optionsMap.get("failureEmailsOverride"));
		}
		
		return options;
	}
}
