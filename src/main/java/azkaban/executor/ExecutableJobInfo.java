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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.utils.Pair;

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

  public ExecutableJobInfo(int execId, int projectId, int version,
      String flowId, String jobId, long startTime, long endTime, Status status,
      int attempt) {
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
    return projectId;
  }

  public int getExecId() {
    return execId;
  }

  public int getVersion() {
    return version;
  }

  public String getFlowId() {
    return flowId;
  }

  public String getImmediateFlowId() {
    if (jobPath.size() == 1) {
      return flowId;
    }
    Pair<String, String> pair = jobPath.get(jobPath.size() - 1);
    return pair.getSecond();
  }

  public String getHeadFlowId() {
    Pair<String, String> pair = jobPath.get(0);

    return pair.getFirst();
  }

  public String getJobId() {
    return jobId;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public Status getStatus() {
    return status;
  }

  public int getAttempt() {
    return attempt;
  }

  public List<Pair<String, String>> getParsedFlowId() {
    return jobPath;
  }

  private void parseFlowId() {
    jobPath = new ArrayList<Pair<String, String>>();
    String[] flowPairs = flowId.split(",");

    for (String flowPair : flowPairs) {
      String[] pairSplit = flowPair.split(":");
      Pair<String, String> pair;
      if (pairSplit.length == 1) {
        pair = new Pair<String, String>(pairSplit[0], pairSplit[0]);
      } else {
        pair = new Pair<String, String>(pairSplit[0], pairSplit[1]);
      }

      jobPath.add(pair);
    }
  }

  public String getJobIdPath() {
    // Skip the first one because it's always just the root.
    String path = "";
    for (int i = 1; i < jobPath.size(); ++i) {
      Pair<String, String> pair = jobPath.get(i);
      path += pair.getFirst() + ":";
    }

    path += jobId;
    return path;
  }

  public Map<String, Object> toObject() {
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("execId", execId);
    map.put("version", version);
    map.put("flowId", flowId);
    map.put("jobId", jobId);
    map.put("startTime", startTime);
    map.put("endTime", endTime);
    map.put("status", status.toString());
    map.put("attempt", attempt);

    return map;
  }
}
