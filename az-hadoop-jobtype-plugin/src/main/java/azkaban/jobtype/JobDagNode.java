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

package azkaban.jobtype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class JobDagNode {
  protected String name;

  protected List<String> parents = new ArrayList<String>();
  protected List<String> successors = new ArrayList<String>();

  protected MapReduceJobState mapReduceJobState;
  protected Properties jobConfiguration;

  protected int level = 0;

  public JobDagNode() {
  }

  public JobDagNode(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public void addParent(JobDagNode parent) {
    parents.add(parent.getName());
  }

  public void setParents(List<String> parents) {
    this.parents = parents;
  }

  public List<String> getParents() {
    return parents;
  }

  public void addSuccessor(JobDagNode successor) {
    successors.add(successor.getName());
  }

  public void setSuccessors(List<String> successors) {
    this.successors = successors;
  }

  public List<String> getSuccessors() {
    return successors;
  }

  public void setMapReduceJobState(MapReduceJobState mapReduceJobState) {
    this.mapReduceJobState = mapReduceJobState;
  }

  public MapReduceJobState getMapReduceJobState() {
    return mapReduceJobState;
  }

  public void setJobConfiguration(Properties jobConfiguration) {
    this.jobConfiguration = jobConfiguration;
  }

  public Properties getJobConfiguration() {
    return jobConfiguration;
  }

  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("name", name);
    jsonObj.put("level", Integer.toString(level));
    jsonObj.put("parents", parents);
    jsonObj.put("successors", successors);
    if (jobConfiguration != null) {
      jsonObj.put("jobConfiguration",
          StatsUtils.propertiesToJson(jobConfiguration));
    }
    if (mapReduceJobState != null) {
      jsonObj.put("mapReduceJobState", mapReduceJobState.toJson());
    }
    return jsonObj;
  }

  @SuppressWarnings("unchecked")
  public static JobDagNode fromJson(Object obj) throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    String name = (String) jsonObj.get("name");

    JobDagNode node = new JobDagNode(name);
    node.setParents((ArrayList<String>) jsonObj.get("parents"));
    node.setSuccessors((ArrayList<String>) jsonObj.get("successors"));
    node.setLevel(Integer.parseInt((String) jsonObj.get("level")));

    // Grab configuration if it is available.
    if (jsonObj.containsKey("jobConfiguration")) {
      node.setJobConfiguration(StatsUtils.propertiesFromJson(jsonObj
          .get("jobConfiguration")));
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
