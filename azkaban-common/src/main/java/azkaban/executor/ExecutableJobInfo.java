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

import azkaban.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutableJobInfo {

  private final int execId;
  private final int projectId;
  private final int version;
  private final String flowId;
  private final String jobId;
  private final long startTime;
  private final long endTime;
  private final Status status;
  private final int attempt;

  private ArrayList<Pair<String, String>> jobPath;
  private String immediateFlowId;

  public ExecutableJobInfo(final int execId, final int projectId, final int version,
      final String flowId, final String jobId, final long startTime, final long endTime,
      final Status status,
      final int attempt) {
    this.execId = execId;
    this.projectId = projectId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = status;
    this.version = version;
    this.flowId = flowId;
    this.jobId = jobId;
    this.attempt = attempt;

    parseFlowId();
  }

  public int getProjectId() {
    return this.projectId;
  }

  public int getExecId() {
    return this.execId;
  }

  public int getVersion() {
    return this.version;
  }

  public String getFlowId() {
    return this.flowId;
  }

  public String getImmediateFlowId() {
    return this.immediateFlowId;
  }

  public String getJobId() {
    return this.jobId;
  }

  public long getStartTime() {
    return this.startTime;
  }

  public long getEndTime() {
    return this.endTime;
  }

  public Status getStatus() {
    return this.status;
  }

  public int getAttempt() {
    return this.attempt;
  }

  public List<Pair<String, String>> getParsedFlowId() {
    return this.jobPath;
  }

  private void parseFlowId() {
    this.jobPath = new ArrayList<>();
    // parsing pattern: flowRootName[,embeddedFlowName:embeddedFlowPath]*
    final String[] flowPairs = this.flowId.split(",");

    for (final String flowPair : flowPairs) {
      // splitting each embeddedFlowName:embeddedFlowPath pair by the first occurrence of ':'
      // only because embeddedFlowPath also uses ':' as delimiter.
      // Ex: "embeddedFlow3:rootFlow:embeddedFlow1:embeddedFlow2:embeddedFlow3" will result in
      // ["embeddedFlow3", "rootFlow:embeddedFlow1:embeddedFlow2:embeddedFlow3"]
      final String[] pairSplit = flowPair.split(":", 2);
      final Pair<String, String> pair;
      if (pairSplit.length == 1) {
        pair = new Pair<>(pairSplit[0], pairSplit[0]);
      } else {
        pair = new Pair<>(pairSplit[0], pairSplit[1]);
      }

      this.jobPath.add(pair);
    }

    this.immediateFlowId = this.jobPath.get(this.jobPath.size() - 1).getSecond();
  }

  public String getJobIdPath() {
    // Skip the first one because it's always just the root.
    String path = "";
    for (int i = 1; i < this.jobPath.size(); ++i) {
      final Pair<String, String> pair = this.jobPath.get(i);
      path += pair.getFirst() + ":";
    }

    path += this.jobId;
    return path;
  }

  public Map<String, Object> toObject() {
    final HashMap<String, Object> map = new HashMap<>();
    map.put("execId", this.execId);
    map.put("version", this.version);
    map.put("flowId", this.flowId);
    map.put("jobId", this.jobId);
    map.put("startTime", this.startTime);
    map.put("endTime", this.endTime);
    map.put("status", this.status.toString());
    map.put("attempt", this.attempt);

    return map;
  }
}
