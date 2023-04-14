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

import static azkaban.Constants.EventReporterConstants.EXECUTION_RETRIED_BY_AZKABAN;
import static azkaban.Constants.EventReporterConstants.ORIGINAL_FLOW_EXECUTION_ID_BEFORE_RETRY;

import azkaban.executor.mail.DefaultMailCreator;
import azkaban.sla.SlaOption;
import azkaban.utils.TypedMapWrapper;
import com.google.common.collect.ImmutableMap;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public static final int DEFAULT_FLOW_RESTART_LIMIT = 2;

  private static final String FLOW_PARAMETERS = "flowParameters";
  private static final String RUNTIME_PROPERTIES = "runtimeProperties";
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
  /**
   * Setting value to true would mean that we should consider the user
   * specified failure.action (through API or UI) than failure.action specified
   * in the flow file.
   */
  public static final String FAILURE_ACTION_OVERRIDE = "failureActionOverride";
  private static final String MAIL_CREATOR = "mailCreator";
  private static final String MEMORY_CHECK = "memoryCheck";
  private boolean isExecutionRetried = false;
  private Integer originalFlowExecutionIdBeforeRetry = null;

  private boolean notifyOnFirstFailure = true;
  private boolean notifyOnLastFailure = false;
  private boolean failureEmailsOverride = false;
  private boolean successEmailsOverride = false;
  private boolean failureActionOverride = false;
  private ArrayList<String> failureEmails = new ArrayList<>();
  private ArrayList<String> successEmails = new ArrayList<>();

  private Integer pipelineLevel = null;
  private Integer pipelineExecId = null;
  private Integer queueLevel = 0;
  private String concurrentOption = CONCURRENT_OPTION_IGNORE;
  private String mailCreator = DefaultMailCreator.DEFAULT_MAIL_CREATOR;
  private boolean memoryCheck = true;
  private Map<String, String> flowParameters = new HashMap<>();
  private Map<String, Map<String, String>> runtimeProperties = new HashMap<>();
  private FailureAction failureAction = FailureAction.FINISH_CURRENTLY_RUNNING;
  private List<DisabledJob> initiallyDisabledJobs = new ArrayList<>();
  private List<SlaOption> slaOptions = new ArrayList<>();
  private Map<String, FailureAction> FAILURE_OPTION_TO_FAILURE_ACTION_MAP =
      ImmutableMap.of(
          "finish_currently_running", FailureAction.FINISH_CURRENTLY_RUNNING,
          "cancel_all", FailureAction.CANCEL_ALL,
          "finish_all_possible", FailureAction.FINISH_ALL_POSSIBLE
      );

  public static ExecutionOptions createFromObject(final Object obj) {
    if (obj == null || !(obj instanceof Map)) {
      return null;
    }

    final Map<String, Object> optionsMap = (Map<String, Object>) obj;
    final TypedMapWrapper<String, Object> wrapper =
        new TypedMapWrapper<>(optionsMap);

    final ExecutionOptions options = new ExecutionOptions();
    if (optionsMap.containsKey(FLOW_PARAMETERS)) {
      options.flowParameters = new HashMap<>();
      options.flowParameters.putAll(wrapper
          .<String, String>getMap(FLOW_PARAMETERS));
    }
    if (optionsMap.containsKey(RUNTIME_PROPERTIES)) {
      options.runtimeProperties = new HashMap<>();
      options.runtimeProperties.putAll(wrapper.getMap(RUNTIME_PROPERTIES));
    }
    // Failure notification
    options.notifyOnFirstFailure =
        wrapper.getBool(NOTIFY_ON_FIRST_FAILURE, options.notifyOnFirstFailure);
    options.notifyOnLastFailure =
        wrapper.getBool(NOTIFY_ON_LAST_FAILURE, options.notifyOnLastFailure);
    options.concurrentOption =
        wrapper.getString(CONCURRENT_OPTION, options.concurrentOption);

    if (wrapper.containsKey(DISABLE)) {
      options.initiallyDisabledJobs = DisabledJob.fromDeprecatedObjectList(wrapper
          .<Object>getList(DISABLE));
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
    options.setSuccessEmails(wrapper.<String>getList(SUCCESS_EMAILS,
        Collections.<String>emptyList()));
    options.setFailureEmails(wrapper.<String>getList(FAILURE_EMAILS,
        Collections.<String>emptyList()));

    options.setSuccessEmailsOverridden(wrapper.getBool(SUCCESS_EMAILS_OVERRIDE,
        false));
    options.setFailureEmailsOverridden(wrapper.getBool(FAILURE_EMAILS_OVERRIDE,
        false));
    options.setFailureActionOverride(wrapper.getBool(FAILURE_ACTION_OVERRIDE,
        false));
    options.setMemoryCheck(wrapper.getBool(MEMORY_CHECK, true));

    // Note: slaOptions was originally outside of execution options, so it parsed and set
    // separately for the original JSON format. New formats should include slaOptions as
    // part of execution options.

    options.setExecutionRetried(wrapper.getBool(EXECUTION_RETRIED_BY_AZKABAN, false));
    options.setOriginalFlowExecutionIdBeforeRetry(wrapper.getInt(ORIGINAL_FLOW_EXECUTION_ID_BEFORE_RETRY,
        options.originalFlowExecutionIdBeforeRetry));

    return options;
  }


  public void addAllFlowParameters(final Map<String, String> flowParam) {
    this.flowParameters.putAll(flowParam);
  }

  public void addAllRuntimeProperties(final Map<String, Map<String, String>> runtimeProperties) {
    this.runtimeProperties.putAll(runtimeProperties);
  }

  public Map<String, String> getFlowParameters() {
    return this.flowParameters;
  }

  public Map<String, Map<String, String>> getRuntimeProperties() {
    return this.runtimeProperties;
  }

  public boolean isFailureEmailsOverridden() {
    return this.failureEmailsOverride;
  }

  public void setFailureEmailsOverridden(final boolean override) {
    this.failureEmailsOverride = override;
  }

  public boolean isSuccessEmailsOverridden() {
    return this.successEmailsOverride;
  }

  public void setSuccessEmailsOverridden(final boolean override) {
    this.successEmailsOverride = override;
  }

  public boolean isFailureActionOverridden() {
    return failureActionOverride;
  }

  public void setFailureActionOverride(final boolean failureActionOverride) {
    this.failureActionOverride = failureActionOverride;
  }


  public List<String> getFailureEmails() {
    return this.failureEmails;
  }

  public void setFailureEmails(final Collection<String> emails) {
    this.failureEmails = new ArrayList<>(emails);
  }

  public List<String> getSuccessEmails() {
    return this.successEmails;
  }

  public void setSuccessEmails(final Collection<String> emails) {
    this.successEmails = new ArrayList<>(emails);
  }

  public boolean getNotifyOnFirstFailure() {
    return this.notifyOnFirstFailure;
  }

  public void setNotifyOnFirstFailure(final boolean notify) {
    this.notifyOnFirstFailure = notify;
  }

  public boolean getNotifyOnLastFailure() {
    return this.notifyOnLastFailure;
  }

  public void setNotifyOnLastFailure(final boolean notify) {
    this.notifyOnLastFailure = notify;
  }

  public FailureAction getFailureAction() {
    return this.failureAction;
  }

  public void setFailureAction(final FailureAction action) {
    this.failureAction = action;
  }

  public String getConcurrentOption() {
    return this.concurrentOption;
  }

  public void setConcurrentOption(final String concurrentOption) {
    this.concurrentOption = concurrentOption;
  }

  public String getMailCreator() {
    return this.mailCreator;
  }

  public void setMailCreator(final String mailCreator) {
    this.mailCreator = mailCreator;
  }

  public Integer getPipelineLevel() {
    return this.pipelineLevel;
  }

  public void setPipelineLevel(final Integer level) {
    this.pipelineLevel = level;
  }

  public Integer getPipelineExecutionId() {
    return this.pipelineExecId;
  }

  public void setPipelineExecutionId(final Integer id) {
    this.pipelineExecId = id;
  }

  public Integer getQueueLevel() {
    return this.queueLevel;
  }

  public List<DisabledJob> getDisabledJobs() {
    return new ArrayList<>(this.initiallyDisabledJobs);
  }

  public void setDisabledJobs(final List<DisabledJob> disabledJobs) {
    this.initiallyDisabledJobs = disabledJobs;
  }

  public boolean getMemoryCheck() {
    return this.memoryCheck;
  }

  public void setMemoryCheck(final boolean memoryCheck) {
    this.memoryCheck = memoryCheck;
  }

  public List<SlaOption> getSlaOptions() {
    return this.slaOptions;
  }

  public void setSlaOptions(final List<SlaOption> slaOptions) {
    this.slaOptions = slaOptions;
  }

  public boolean isExecutionRetried() { return this.isExecutionRetried; }

  public void setExecutionRetried(boolean executionRetried) { this.isExecutionRetried =
      executionRetried; }

  public Integer getOriginalFlowExecutionIdBeforeRetry() { return originalFlowExecutionIdBeforeRetry; }

  public void setOriginalFlowExecutionIdBeforeRetry(Integer originalFlowExecutionIdBeforeRetry) { this.originalFlowExecutionIdBeforeRetry =
      originalFlowExecutionIdBeforeRetry; }

  public Map<String, Object> toObject() {
    final HashMap<String, Object> flowOptionObj = new HashMap<>();

    flowOptionObj.put(FLOW_PARAMETERS, this.flowParameters);
    flowOptionObj.put(RUNTIME_PROPERTIES, this.runtimeProperties);
    flowOptionObj.put(NOTIFY_ON_FIRST_FAILURE, this.notifyOnFirstFailure);
    flowOptionObj.put(NOTIFY_ON_LAST_FAILURE, this.notifyOnLastFailure);
    flowOptionObj.put(SUCCESS_EMAILS, this.successEmails);
    flowOptionObj.put(FAILURE_EMAILS, this.failureEmails);
    flowOptionObj.put(FAILURE_ACTION, this.failureAction.toString());
    flowOptionObj.put(PIPELINE_LEVEL, this.pipelineLevel);
    flowOptionObj.put(PIPELINE_EXECID, this.pipelineExecId);
    flowOptionObj.put(QUEUE_LEVEL, this.queueLevel);
    flowOptionObj.put(CONCURRENT_OPTION, this.concurrentOption);
    flowOptionObj.put(DISABLE, DisabledJob.toDeprecatedObjectList(this.initiallyDisabledJobs));
    flowOptionObj.put(FAILURE_EMAILS_OVERRIDE, this.failureEmailsOverride);
    flowOptionObj.put(SUCCESS_EMAILS_OVERRIDE, this.successEmailsOverride);
    flowOptionObj.put(FAILURE_ACTION_OVERRIDE, this.failureActionOverride);
    flowOptionObj.put(MAIL_CREATOR, this.mailCreator);
    flowOptionObj.put(MEMORY_CHECK, this.memoryCheck);
    flowOptionObj.put(EXECUTION_RETRIED_BY_AZKABAN, this.isExecutionRetried);
    flowOptionObj.put(ORIGINAL_FLOW_EXECUTION_ID_BEFORE_RETRY, this.originalFlowExecutionIdBeforeRetry);
    return flowOptionObj;
  }

  public String toJSON() {
    return new GsonBuilder().setPrettyPrinting().create().toJson(toObject());
  }

  /**
   * Read the failure option value and return the corresponding
   * Enum object.
   * @param failureAction this value is passed from the DSL/Yaml file
   * @return Azkaban understood FailureAction object
   */
  public FailureAction mapToFailureAction(final String failureAction) {
    return FAILURE_OPTION_TO_FAILURE_ACTION_MAP.getOrDefault(
        failureAction, FailureAction.FINISH_CURRENTLY_RUNNING
    );
  }

  /**
   * Merge "this" with the input {@link ExecutionOptions}, for every the fields being set in input;
   * If conflicts, the values set in the input will get to overwrite the old values.
   * @return
   */
  public ExecutionOptions merge(ExecutionOptions overwriteOptions) {
    // TODO: implement it
    return overwriteOptions;
  }

  public enum FailureAction {
    FINISH_CURRENTLY_RUNNING, CANCEL_ALL, FINISH_ALL_POSSIBLE;
  }
}
