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

import azkaban.DispatchMethod;
import azkaban.flow.Flow;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.project.Project;
import azkaban.sla.SlaOption;
import azkaban.utils.Props;
import azkaban.utils.TypedMapWrapper;
import com.sun.istack.NotNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


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
  public static final String SLAOPTIONS_PARAM = "slaOptions";
  public static final String AZKABANFLOWVERSION_PARAM = "azkabanFlowVersion";
  public static final String IS_LOCKED_PARAM = "isLocked";
  public static final String FLOW_LOCK_ERROR_MESSAGE_PARAM = "flowLockErrorMessage";
  public static final String EXECUTION_SOURCE = "executionSource";
  public static final String FLOW_DISPATCH_METHOD = "dispatch_method";
  public static final String VERSIONSET_JSON_PARAM = "version set json";
  public static final String VERSIONSET_MD5HEX_PARAM = "version set Md5Hex";
  public static final String VERSIONSET_ID_PARAM = "version set id";

  private final HashSet<String> proxyUsers = new HashSet<>();
  private int executionId = -1;
  private int scheduleId = -1;
  private int projectId;
  private String executionSource;
  private String projectName;
  private String lastModifiedUser;
  private int version;
  private long submitTime = -1;
  private long lastModifiedTimestamp;
  private String submitUser;
  private String executionPath;
  private ExecutionOptions executionOptions;
  private double azkabanFlowVersion;
  private boolean isLocked;
  private ExecutableFlowRampMetadata executableFlowRampMetadata;
  private String flowLockErrorMessage;
  // For Flow_Status_Changed event
  private String failedJobId = "unknown";
  private String modifiedBy = "unknown";
  private DispatchMethod dispatchMethod;

  // For slaOption information
  private String slaOptionStr = "null";

  // For Flows dispatched from a k8s pod
  private VersionSet versionSet;

  public ExecutableFlow(final Project project, final Flow flow) {
    this.projectId = project.getId();
    this.projectName = project.getName();
    this.version = project.getVersion();
    this.scheduleId = -1;
    this.lastModifiedTimestamp = project.getLastModifiedTimestamp();
    this.lastModifiedUser = project.getLastModifiedUser();
    setAzkabanFlowVersion(flow.getAzkabanFlowVersion());
    setLocked(flow.isLocked());
    setFlowLockErrorMessage(flow.getFlowLockErrorMessage());
    this.setFlow(project, flow);
  }

  public ExecutableFlow() {
  }

  public static ExecutableFlow createExecutableFlow(final Object obj, final Status status) {
    final ExecutableFlow exFlow = new ExecutableFlow();
    final HashMap<String, Object> flowObj = (HashMap<String, Object>) obj;
    exFlow.fillExecutableFromMapObject(flowObj);
    // overwrite status from the flow data blob as that one should NOT be used
    exFlow.setStatus(status);
    return exFlow;
  }

  public DispatchMethod getDispatchMethod() {
    return this.dispatchMethod;
  }

  public void setDispatchMethod(final DispatchMethod dispatchMethod) {
    this.dispatchMethod = dispatchMethod;
  }

  @Override
  public String getId() {
    return getFlowId();
  }

  @Override
  public ExecutableFlow getExecutableFlow() {
    return this;
  }

  public void addAllProxyUsers(final Collection<String> proxyUsers) {
    this.proxyUsers.addAll(proxyUsers);
  }

  public Set<String> getProxyUsers() {
    return new HashSet<>(this.proxyUsers);
  }

  public ExecutionOptions getExecutionOptions() {
    return this.executionOptions;
  }

  public void setExecutionOptions(final ExecutionOptions options) {
    this.executionOptions = options;
  }

  @Override
  protected void setFlow(final Project project, final Flow flow) {
    super.setFlow(project, flow);
    this.executionOptions = new ExecutionOptions();
    this.executionOptions.setMailCreator(flow.getMailCreator());

    if (flow.getSuccessEmails() != null) {
      this.executionOptions.setSuccessEmails(flow.getSuccessEmails());
    }
    if (flow.getFailureEmails() != null) {
      this.executionOptions.setFailureEmails(flow.getFailureEmails());
    }
  }

  @Override
  public int getExecutionId() {
    return this.executionId;
  }

  public void setExecutionId(final int executionId) {
    this.executionId = executionId;
  }

  @Override
  public long getLastModifiedTimestamp() {
    return this.lastModifiedTimestamp;
  }

  public void setLastModifiedTimestamp(final long lastModifiedTimestamp) {
    this.lastModifiedTimestamp = lastModifiedTimestamp;
  }

  @Override
  public String getLastModifiedByUser() {
    return this.lastModifiedUser;
  }

  public void setLastModifiedByUser(final String lastModifiedUser) {
    this.lastModifiedUser = lastModifiedUser;
  }

  @Override
  public int getProjectId() {
    return this.projectId;
  }

  public void setProjectId(final int projectId) {
    this.projectId = projectId;
  }

  @Override
  public String getExecutionSource() {
    return this.executionSource;
  }

  public void setExecutionSource(final String executionSource) {
    this.executionSource = executionSource;
  }

  @Override
  public String getProjectName() {
    return this.projectName;
  }

  public int getScheduleId() {
    return this.scheduleId;
  }

  public void setScheduleId(final int scheduleId) {
    this.scheduleId = scheduleId;
  }

  public String getExecutionPath() {
    return this.executionPath;
  }

  public void setExecutionPath(final String executionPath) {
    this.executionPath = executionPath;
  }

  public String getSubmitUser() {
    return this.submitUser;
  }

  public void setSubmitUser(final String submitUser) {
    this.submitUser = submitUser;
  }

  @Override
  public int getVersion() {
    return this.version;
  }

  public void setVersion(final int version) {
    this.version = version;
  }

  public long getSubmitTime() {
    return this.submitTime;
  }

  public void setSubmitTime(final long submitTime) {
    this.submitTime = submitTime;
  }

  public double getAzkabanFlowVersion() {
    return this.azkabanFlowVersion;
  }

  public void setAzkabanFlowVersion(final double azkabanFlowVersion) {
    this.azkabanFlowVersion = azkabanFlowVersion;
  }

  public boolean isLocked() { return this.isLocked; }

  public void setLocked(boolean locked) { this.isLocked = locked; }

  public String getFlowLockErrorMessage() {
    return this.flowLockErrorMessage;
  }

  public void setFlowLockErrorMessage(final String flowLockErrorMessage) {
    this.flowLockErrorMessage = flowLockErrorMessage;
  }

  public String getSlaOptionStr() {
    return slaOptionStr;
  }

  @Override
  public Map<String, Object> toObject() {
    final HashMap<String, Object> flowObj = new HashMap<>();
    fillMapFromExecutable(flowObj);

    flowObj.put(EXECUTIONID_PARAM, this.executionId);
    flowObj.put(EXECUTIONPATH_PARAM, this.executionPath);
    flowObj.put(PROJECTID_PARAM, this.projectId);
    flowObj.put(PROJECTNAME_PARAM, this.projectName);

    if (this.scheduleId >= 0) {
      flowObj.put(SCHEDULEID_PARAM, this.scheduleId);
    }

    flowObj.put(SUBMITUSER_PARAM, this.submitUser);
    flowObj.put(EXECUTION_SOURCE, this.executionSource);
    flowObj.put(VERSION_PARAM, this.version);
    flowObj.put(LASTMODIFIEDTIME_PARAM, this.lastModifiedTimestamp);
    flowObj.put(LASTMODIFIEDUSER_PARAM, this.lastModifiedUser);
    flowObj.put(AZKABANFLOWVERSION_PARAM, this.azkabanFlowVersion);

    flowObj.put(EXECUTIONOPTIONS_PARAM, this.executionOptions.toObject());

    final ArrayList<String> proxyUserList = new ArrayList<>(this.proxyUsers);
    flowObj.put(PROXYUSERS_PARAM, proxyUserList);

    flowObj.put(SUBMITTIME_PARAM, this.submitTime);

    final List<Map<String, Object>> slaOptions = new ArrayList<>();
    List<SlaOption> slaOptionList = this.executionOptions.getSlaOptions();
    if (slaOptionList != null) {
      for (SlaOption slaOption : slaOptionList) {
        slaOptions.add(slaOption.toObject());
      }
    }

    flowObj.put(SLAOPTIONS_PARAM, slaOptions);

    flowObj.put(IS_LOCKED_PARAM, this.isLocked);
    flowObj.put(FLOW_LOCK_ERROR_MESSAGE_PARAM, this.flowLockErrorMessage);
    flowObj.put(FLOW_DISPATCH_METHOD, getDispatchMethod().getNumVal());

    if (this.getVersionSet() != null) {
      flowObj.put(VERSIONSET_JSON_PARAM, this.getVersionSet().getVersionSetJsonString());
      flowObj.put(VERSIONSET_MD5HEX_PARAM, this.getVersionSet().getVersionSetMd5Hex());
      flowObj.put(VERSIONSET_ID_PARAM, this.getVersionSet().getVersionSetId());
    }

    return flowObj;
  }

  @Override
  public void fillExecutableFromMapObject(
      final TypedMapWrapper<String, Object> flowObj) {
    super.fillExecutableFromMapObject(flowObj);

    this.executionId = flowObj.getInt(EXECUTIONID_PARAM);
    this.executionPath = flowObj.getString(EXECUTIONPATH_PARAM);

    this.projectId = flowObj.getInt(PROJECTID_PARAM);
    this.projectName = flowObj.getString(PROJECTNAME_PARAM);
    this.executionSource = flowObj.getString(EXECUTION_SOURCE);
    this.scheduleId = flowObj.getInt(SCHEDULEID_PARAM);
    this.submitUser = flowObj.getString(SUBMITUSER_PARAM);
    this.version = flowObj.getInt(VERSION_PARAM);
    this.lastModifiedTimestamp = flowObj.getLong(LASTMODIFIEDTIME_PARAM);
    this.lastModifiedUser = flowObj.getString(LASTMODIFIEDUSER_PARAM);
    this.submitTime = flowObj.getLong(SUBMITTIME_PARAM);
    this.azkabanFlowVersion = flowObj.getDouble(AZKABANFLOWVERSION_PARAM);

    if (flowObj.containsKey(EXECUTIONOPTIONS_PARAM)) {
      this.executionOptions =
          ExecutionOptions.createFromObject(flowObj
              .getObject(EXECUTIONOPTIONS_PARAM));
    } else {
      // for backwards compatibility should remove in a few versions.
      this.executionOptions = ExecutionOptions.createFromObject(flowObj);
    }

    if (flowObj.containsKey(PROXYUSERS_PARAM)) {
      final List<String> proxyUserList = flowObj.<String>getList(PROXYUSERS_PARAM);
      this.addAllProxyUsers(proxyUserList);
    }

    if (flowObj.containsKey(SLAOPTIONS_PARAM)) {
      final List<SlaOption> slaOptions =
          flowObj.getList(SLAOPTIONS_PARAM).stream().map(SlaOption::fromObject)
              .collect(Collectors.toList());
      this.executionOptions.setSlaOptions(slaOptions);
      // Fill slaOptionStr a comma delimited String of slaOptions
      StringBuilder slaBuilder = new StringBuilder();
      for (SlaOption slaOption: slaOptions){
        slaBuilder.append(slaOption.toString());
        slaBuilder.append(';');
      }
      this.slaOptionStr = slaBuilder.toString();
    }

    if (flowObj.containsKey(VERSIONSET_JSON_PARAM) && flowObj.containsKey(VERSIONSET_MD5HEX_PARAM) && flowObj.containsKey(VERSIONSET_ID_PARAM)) {
      final String versionSetJsonString = flowObj.getString(VERSIONSET_JSON_PARAM);
      final String versionSetMd5Hex = flowObj.getString(VERSIONSET_MD5HEX_PARAM);
      final int versionSetId = flowObj.getInt(VERSIONSET_ID_PARAM);
      final VersionSet versionSet = new VersionSet(versionSetJsonString, versionSetMd5Hex,
          versionSetId);
      this.setVersionSet(versionSet);
    }

    this.setLocked(flowObj.getBool(IS_LOCKED_PARAM, false));
    this.setFlowLockErrorMessage(flowObj.getString(FLOW_LOCK_ERROR_MESSAGE_PARAM, null));
    // Dispatch Method default is POLL
    this.setDispatchMethod(DispatchMethod.fromNumVal(flowObj.getInt(FLOW_DISPATCH_METHOD,
        DispatchMethod.POLL.getNumVal())));
  }

  @Override
  public Map<String, Object> toUpdateObject(final long lastUpdateTime) {
    final Map<String, Object> updateData = super.toUpdateObject(lastUpdateTime);
    updateData.put(EXECUTIONID_PARAM, this.executionId);
    return updateData;
  }

  @Override
  public void resetForRetry() {
    super.resetForRetry();
    this.setStatus(Status.RUNNING);
  }

  public ExecutableFlowRampMetadata getExecutableFlowRampMetadata() {
    return executableFlowRampMetadata;
  }

  public void setExecutableFlowRampMetadata(ExecutableFlowRampMetadata executableFlowRampMetadata) {
    this.executableFlowRampMetadata = executableFlowRampMetadata;
  }

  /**
   * Get the Relative Flow Directory against project directory
   */
  public String getDirectory() {
    return String.valueOf(getProjectId()) + "." + String.valueOf(getVersion());
  }

  /**
   * Get Ramp Props For Job
   * @param jobId job Id
   * @param jobType jobType aka job plugin type
   * @return ramp Props
   */
  synchronized public Props getRampPropsForJob(@NotNull final String jobId, @NotNull final String jobType) {
    return Optional.ofNullable(executableFlowRampMetadata)
        .map(metadata -> metadata.selectRampPropsForJob(jobId, jobType))
        .orElse(null);
  }

  public void setFailedJobId(final String id) {
     this.failedJobId = id;
  }

  public String getFailedJobId() {
    return failedJobId;
  }

  @Override
  public String getModifiedBy() { return modifiedBy; }

  @Override
  public void setModifiedBy(final String id) { this.modifiedBy = id; }

  public VersionSet getVersionSet() {
    return versionSet;
  }

  public void setVersionSet(final VersionSet versionSet) {
    this.versionSet = versionSet;
  }
}
