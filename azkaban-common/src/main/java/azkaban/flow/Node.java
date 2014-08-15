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

package azkaban.flow;

import java.awt.geom.Point2D;
import java.util.HashMap;
import java.util.Map;

import azkaban.utils.Utils;

public class Node {
  private final String id;
  private String jobSource;
  private String propsSource;

  private Point2D position = null;
  private int level;
  private int expectedRunTimeSec = 1;
  private String type;

  private String embeddedFlowId;

  public Node(String id) {
    this.id = id;
  }

  /**
   * Clones nodes
   *
   * @param node
   */
  public Node(Node clone) {
    this.id = clone.id;
    this.propsSource = clone.propsSource;
    this.jobSource = clone.jobSource;
  }

  public String getId() {
    return id;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Point2D getPosition() {
    return position;
  }

  public void setPosition(Point2D position) {
    this.position = position;
  }

  public void setPosition(double x, double y) {
    this.position = new Point2D.Double(x, y);
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public String getJobSource() {
    return jobSource;
  }

  public void setJobSource(String jobSource) {
    this.jobSource = jobSource;
  }

  public String getPropsSource() {
    return propsSource;
  }

  public void setPropsSource(String propsSource) {
    this.propsSource = propsSource;
  }

  public void setExpectedRuntimeSec(int runtimeSec) {
    expectedRunTimeSec = runtimeSec;
  }

  public int getExpectedRuntimeSec() {
    return expectedRunTimeSec;
  }

  public void setEmbeddedFlowId(String flowId) {
    embeddedFlowId = flowId;
  }

  public String getEmbeddedFlowId() {
    return embeddedFlowId;
  }

  @SuppressWarnings("unchecked")
  public static Node fromObject(Object obj) {
    Map<String, Object> mapObj = (Map<String, Object>) obj;
    String id = (String) mapObj.get("id");

    Node node = new Node(id);
    String jobSource = (String) mapObj.get("jobSource");
    String propSource = (String) mapObj.get("propSource");
    String jobType = (String) mapObj.get("jobType");

    String embeddedFlowId = (String) mapObj.get("embeddedFlowId");

    node.setJobSource(jobSource);
    node.setPropsSource(propSource);
    node.setType(jobType);
    node.setEmbeddedFlowId(embeddedFlowId);

    Integer expectedRuntime = (Integer) mapObj.get("expectedRuntime");
    if (expectedRuntime != null) {
      node.setExpectedRuntimeSec(expectedRuntime);
    }

    Map<String, Object> layoutInfo = (Map<String, Object>) mapObj.get("layout");
    if (layoutInfo != null) {
      Double x = null;
      Double y = null;
      Integer level = null;

      try {
        x = Utils.convertToDouble(layoutInfo.get("x"));
        y = Utils.convertToDouble(layoutInfo.get("y"));
        level = (Integer) layoutInfo.get("level");
      } catch (ClassCastException e) {
        throw new RuntimeException("Error creating node " + id, e);
      }

      if (x != null && y != null) {
        node.setPosition(new Point2D.Double(x, y));
      }
      if (level != null) {
        node.setLevel(level);
      }
    }

    return node;
  }

  public Object toObject() {
    HashMap<String, Object> objMap = new HashMap<String, Object>();
    objMap.put("id", id);
    objMap.put("jobSource", jobSource);
    objMap.put("propSource", propsSource);
    objMap.put("jobType", type);
    if (embeddedFlowId != null) {
      objMap.put("embeddedFlowId", embeddedFlowId);
    }
    objMap.put("expectedRuntime", expectedRunTimeSec);

    HashMap<String, Object> layoutInfo = new HashMap<String, Object>();
    if (position != null) {
      layoutInfo.put("x", position.getX());
      layoutInfo.put("y", position.getY());
    }
    layoutInfo.put("level", level);
    objMap.put("layout", layoutInfo);

    return objMap;
  }
}
