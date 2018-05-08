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

package azkaban.jobtype.pig;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.jobtype.JobDagNode;
import azkaban.jobtype.MapReduceJobState;
import azkaban.jobtype.StatsUtils;

import org.apache.pig.tools.pigstats.JobStats;

public class PigJobDagNode extends JobDagNode {
  private String jobId;

  private List<String> features;
  private List<String> aliases;

  private PigJobStats jobStats;

  public PigJobDagNode(String name, String[] aliases, String[] features) {
    super(name);
    this.aliases = Arrays.asList(aliases);
    this.features = Arrays.asList(features);
  }

  public PigJobDagNode(String name, List<String> aliases, List<String> features) {
    super(name);
    this.aliases = aliases;
    this.features = features;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public List<String> getAliases() {
    return aliases;
  }

  public List<String> getFeatures() {
    return features;
  }

  public void setJobStats(PigJobStats pigJobStats) {
    this.jobStats = pigJobStats;
  }

  public void setJobStats(JobStats jobStats) {
    this.jobStats = new PigJobStats(jobStats);
  }

  public PigJobStats getJobStats() {
    return jobStats;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("name", name);
    jsonObj.put("jobId", jobId);
    jsonObj.put("level", Integer.toString(level));
    jsonObj.put("aliases", Arrays.asList(aliases));
    jsonObj.put("features", Arrays.asList(features));
    jsonObj.put("parents", parents);
    jsonObj.put("successors", successors);
    if (jobConfiguration != null) {
      jsonObj.put("jobConfiguration",
          StatsUtils.propertiesToJson(jobConfiguration));
    }
    if (jobStats != null) {
      jsonObj.put("jobStats", jobStats.toJson());
    }
    if (mapReduceJobState != null) {
      jsonObj.put("mapReduceJobState", mapReduceJobState.toJson());
    }
    return jsonObj;
  }

  @SuppressWarnings("unchecked")
  public static PigJobDagNode fromJson(Object obj) throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    String name = (String) jsonObj.get("name");
    List<String> aliases = (ArrayList<String>) jsonObj.get("aliases");
    List<String> features = (ArrayList<String>) jsonObj.get("features");

    PigJobDagNode node = new PigJobDagNode(name, aliases, features);
    node.setJobId((String) jsonObj.get("jobId"));
    node.setParents((ArrayList<String>) jsonObj.get("parents"));
    node.setSuccessors((ArrayList<String>) jsonObj.get("successors"));
    node.setLevel(Integer.parseInt((String) jsonObj.get("level")));

    // Grab configuration if it is available.
    if (jsonObj.containsKey("jobConfiguration")) {
      node.setJobConfiguration(StatsUtils.propertiesFromJson(jsonObj
          .get("jobConfiguration")));
    }

    // Grab PigJobStats;
    if (jsonObj.containsKey("jobStats")) {
      PigJobStats pigJobStats = PigJobStats.fromJson(jsonObj.get("jobStats"));
      node.setJobStats(pigJobStats);
    }

    // Grab MapReduceJobState.
    if (jsonObj.containsKey("mapReduceJobState")) {
      MapReduceJobState mapReduceJobState =
          MapReduceJobState.fromJson(jsonObj.get("mapReduceJobState"));
      node.setMapReduceJobState(mapReduceJobState);
    }

    return node;
  }
}
