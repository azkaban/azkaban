/*
 * Copyright 2013 LinkedIn Corp.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.executor.mail.DefaultMailCreator;
import azkaban.utils.TypedMapWrapper;

/**
 * Execution options for submitted flows and scheduled flows
 */
public class ExecutionOptions {
  public static final String CONCURRENT_OPTION_SKIP = "skip";
  public static final String CONCURRENT_OPTION_PIPELINE = "pipeline";
  public static final String CONCURRENT_OPTION_IGNORE = "ignore";
  public static final String FLOW_PRIORITY = "flowPriority";
  /* override dispatcher selection and use executor id specified */
  public static final String USE_EXECUTOR = "useExecutor";
  public static final int DEFAULT_FLOW_PRIORITY = 5;

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
  private static final String MAIL_CREATOR = "mailCreator";
  private static final String MEMORY_CHECK = "memoryCheck";

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
  private boolean memoryCheck = true;
  private Map<String, String> flowParameters = new HashMap<String, String>();

  public enum FailureAction {
    FINISH_CURRENTLY_RUNNING, CANCEL_ALL, FINISH_ALL_POSSIBLE
  }

  private FailureAction failureAction = FailureAction.FINISH_CURRENTLY_RUNNING;

  private List<Object> initiallyDisabledJobs = new ArrayList<Object>();

  public void addAllFlowParameters(Map<String, String> flowParam) {
    flowParameters.putAll(flowParam);
  }

  public Map<String, String> getFlowParameters() {
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

  public List<Object> getDisabledJobs() {
    return new ArrayList<Object>(initiallyDisabledJobs);
  }

  public void setDisabledJobs(List<Object> disabledJobs) {
    initiallyDisabledJobs = disabledJobs;
  }

  public boolean getMemoryCheck() {
    return memoryCheck;
  }

  public void setMemoryCheck(boolean memoryCheck) {
    this.memoryCheck = memoryCheck;
  }

  public Map<String, Object> toObject() {
    HashMap<String, Object> flowOptionObj = new HashMap<String, Object>();

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
    flowOptionObj.put(MAIL_CREATOR, mailCreator);
    flowOptionObj.put(MEMORY_CHECK, memoryCheck);
    return flowOptionObj;
  }

  @SuppressWarnings("unchecked")
  public static ExecutionOptions createFromObject(Object obj) {
    if (obj == null || !(obj instanceof Map)) {
      return null;
    }

    Map<String, Object> optionsMap = (Map<String, Object>) obj;
    TypedMapWrapper<String, Object> wrapper =
        new TypedMapWrapper<String, Object>(optionsMap);

    ExecutionOptions options = new ExecutionOptions();
    if (optionsMap.containsKey(FLOW_PARAMETERS)) {
      options.flowParameters = new HashMap<String, String>();
      options.flowParameters.putAll(wrapper
          .<String, String> getMap(FLOW_PARAMETERS));
    }
    // Failure notification
    options.notifyOnFirstFailure =
        wrapper.getBool(NOTIFY_ON_FIRST_FAILURE, options.notifyOnFirstFailure);
    options.notifyOnLastFailure =
        wrapper.getBool(NOTIFY_ON_LAST_FAILURE, options.notifyOnLastFailure);
    options.concurrentOption =
        wrapper.getString(CONCURRENT_OPTION, options.concurrentOption);

    if (wrapper.containsKey(DISABLE)) {
      options.initiallyDisabledJobs = wrapper.<Object> getList(DISABLE);
    }

    if (optionsMap.containsKey(MAIL_CREATOR)) {
      options.mailCreator = (String) optionsMap.get(MAIL_CREATOR);
    }

    // Failure action
    options.failureAction =
        FailureAction.valueOf(wrapper.getString(FAILURE_ACTION,
            options.failureAction.toString()));
    options.pipelineLevel =
        wrapper.getInt(PIPELINE_LEVEL, options.pipelineLevel);
    options.pipelineExecId =
        wrapper.getInt(PIPELINE_EXECID, options.pipelineExecId);
    options.queueLevel = wrapper.getInt(QUEUE_LEVEL, options.queueLevel);

    // Success emails
    options.setSuccessEmails(wrapper.<String> getList(SUCCESS_EMAILS,
        Collections.<String> emptyList()));
    options.setFailureEmails(wrapper.<String> getList(FAILURE_EMAILS,
        Collections.<String> emptyList()));

    options.setSuccessEmailsOverridden(wrapper.getBool(SUCCESS_EMAILS_OVERRIDE,
        false));
    options.setFailureEmailsOverridden(wrapper.getBool(FAILURE_EMAILS_OVERRIDE,
        false));

    options.setMemoryCheck(wrapper.getBool(MEMORY_CHECK, true));

    return options;
  }
}
