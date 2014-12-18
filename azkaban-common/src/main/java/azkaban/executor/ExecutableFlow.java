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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.utils.TypedMapWrapper;

public class ExecutableFlow extends ExecutableFlowBase {
  public static final String EXECUTIONID_PARAM = "executionId";
  public static final String EXECUTIONPATH_PARAM = "executionPath";
  public static final String EXECUTIONOPTIONS_PARAM = "executionOptions";
  public static final String PROJECTID_PARAM = "projectId";
  public static final String SCHEDULEID_PARAM = "scheduleId";
  public static final String SUBMITUSER_PARAM = "submitUser";
  public static final String SUBMITTIME_PARAM = "submitTime";
  public static final String VERSION_PARAM = "version";
  public static final String PROXYUSERS_PARAM = "proxyUsers";
  public static final String PROJECTNAME_PARAM = "projectName";
  public static final String LASTMODIFIEDTIME_PARAM = "lastModfiedTime";
  public static final String LASTMODIFIEDUSER_PARAM = "lastModifiedUser";


  private int executionId = -1;
  private int scheduleId = -1;
  private int projectId;
  private String projectName;
  private String lastModifiedUser;
  private int version;
  private long submitTime = -1;
  private long lastModifiedTimestamp;
  private String submitUser;
  private String executionPath;

  private HashSet<String> proxyUsers = new HashSet<String>();
  private ExecutionOptions executionOptions;

  public ExecutableFlow(Project project, Flow flow) {
    this.projectId = project.getId();
    this.projectName = project.getName();
    this.version = project.getVersion();
    this.scheduleId = -1;
    this.lastModifiedTimestamp = project.getLastModifiedTimestamp();
    this.lastModifiedUser = project.getLastModifiedUser();
    this.setFlow(project, flow);
  }

  public ExecutableFlow() {
  }

  @Override
  public String getId() {
    return getFlowId();
  }

  @Override
  public ExecutableFlow getExecutableFlow() {
    return this;
  }

  public void addAllProxyUsers(Collection<String> proxyUsers) {
    this.proxyUsers.addAll(proxyUsers);
  }

  public Set<String> getProxyUsers() {
    return new HashSet<String>(this.proxyUsers);
  }

  public void setExecutionOptions(ExecutionOptions options) {
    executionOptions = options;
  }

  public ExecutionOptions getExecutionOptions() {
    return executionOptions;
  }

  @Override
  protected void setFlow(Project project, Flow flow) {
    super.setFlow(project, flow);
    executionOptions = new ExecutionOptions();
    executionOptions.setMailCreator(flow.getMailCreator());

    if (flow.getSuccessEmails() != null) {
      executionOptions.setSuccessEmails(flow.getSuccessEmails());
    }
    if (flow.getFailureEmails() != null) {
      executionOptions.setFailureEmails(flow.getFailureEmails());
    }
  }

  @Override
  public int getExecutionId() {
    return executionId;
  }

  public void setExecutionId(int executionId) {
    this.executionId = executionId;
  }

  @Override
  public long getLastModifiedTimestamp() {
    return lastModifiedTimestamp;
  }

  public void setLastModifiedTimestamp(long lastModifiedTimestamp) {
    this.lastModifiedTimestamp = lastModifiedTimestamp;
  }

  @Override
  public String getLastModifiedByUser() {
    return lastModifiedUser;
  }

  public void setLastModifiedByUser(String lastModifiedUser) {
    this.lastModifiedUser = lastModifiedUser;
  }

  @Override
  public int getProjectId() {
    return projectId;
  }

  public void setProjectId(int projectId) {
    this.projectId = projectId;
  }

  @Override
  public String getProjectName() {
    return projectName;
  }

  public int getScheduleId() {
    return scheduleId;
  }

  public void setScheduleId(int scheduleId) {
    this.scheduleId = scheduleId;
  }

  public String getExecutionPath() {
    return executionPath;
  }

  public void setExecutionPath(String executionPath) {
    this.executionPath = executionPath;
  }

  public String getSubmitUser() {
    return submitUser;
  }

  public void setSubmitUser(String submitUser) {
    this.submitUser = submitUser;
  }

  @Override
  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  @Override
  public Map<String, Object> toObject() {
    HashMap<String, Object> flowObj = new HashMap<String, Object>();
    fillMapFromExecutable(flowObj);

    flowObj.put(EXECUTIONID_PARAM, executionId);
    flowObj.put(EXECUTIONPATH_PARAM, executionPath);
    flowObj.put(PROJECTID_PARAM, projectId);
    flowObj.put(PROJECTNAME_PARAM, projectName);

    if (scheduleId >= 0) {
      flowObj.put(SCHEDULEID_PARAM, scheduleId);
    }

    flowObj.put(SUBMITUSER_PARAM, submitUser);
    flowObj.put(VERSION_PARAM, version);
    flowObj.put(LASTMODIFIEDTIME_PARAM, lastModifiedTimestamp);
    flowObj.put(LASTMODIFIEDUSER_PARAM, lastModifiedUser);

    flowObj.put(EXECUTIONOPTIONS_PARAM, this.executionOptions.toObject());
    flowObj.put(VERSION_PARAM, version);

    ArrayList<String> proxyUserList = new ArrayList<String>(proxyUsers);
    flowObj.put(PROXYUSERS_PARAM, proxyUserList);

    flowObj.put(SUBMITTIME_PARAM, submitTime);

    return flowObj;
  }

  @SuppressWarnings("unchecked")
  public static ExecutableFlow createExecutableFlowFromObject(Object obj) {
    ExecutableFlow exFlow = new ExecutableFlow();
    HashMap<String, Object> flowObj = (HashMap<String, Object>) obj;
    exFlow.fillExecutableFromMapObject(flowObj);

    return exFlow;
  }

  @Override
  public void fillExecutableFromMapObject(
      TypedMapWrapper<String, Object> flowObj) {
    super.fillExecutableFromMapObject(flowObj);

    this.executionId = flowObj.getInt(EXECUTIONID_PARAM);
    this.executionPath = flowObj.getString(EXECUTIONPATH_PARAM);

    this.projectId = flowObj.getInt(PROJECTID_PARAM);
    this.projectName = flowObj.getString(PROJECTNAME_PARAM);
    this.scheduleId = flowObj.getInt(SCHEDULEID_PARAM);
    this.submitUser = flowObj.getString(SUBMITUSER_PARAM);
    this.version = flowObj.getInt(VERSION_PARAM);
    this.lastModifiedTimestamp = flowObj.getLong(LASTMODIFIEDTIME_PARAM);
    this.lastModifiedUser = flowObj.getString(LASTMODIFIEDUSER_PARAM);
    this.submitTime = flowObj.getLong(SUBMITTIME_PARAM);

    if (flowObj.containsKey(EXECUTIONOPTIONS_PARAM)) {
      this.executionOptions =
          ExecutionOptions.createFromObject(flowObj
              .getObject(EXECUTIONOPTIONS_PARAM));
    } else {
      // for backwards compatibility should remove in a few versions.
      this.executionOptions = ExecutionOptions.createFromObject(flowObj);
    }

    if (flowObj.containsKey(PROXYUSERS_PARAM)) {
      List<String> proxyUserList = flowObj.<String> getList(PROXYUSERS_PARAM);
      this.addAllProxyUsers(proxyUserList);

    }
  }

  @Override
  public Map<String, Object> toUpdateObject(long lastUpdateTime) {
    Map<String, Object> updateData = super.toUpdateObject(lastUpdateTime);
    updateData.put(EXECUTIONID_PARAM, this.executionId);
    return updateData;
  }

  @Override
  public void resetForRetry() {
    super.resetForRetry();
    this.setStatus(Status.RUNNING);
  }

}
