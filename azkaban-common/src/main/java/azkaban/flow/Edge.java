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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Edge {
  private final String sourceId;
  private final String targetId;
  private Node source;
  private Node target;
  private String error;

  // Useful in rendering.
  private String guideType;
  private List<Point2D> guideValues;

  public Edge(String fromId, String toId) {
    this.sourceId = fromId;
    this.targetId = toId;
  }

  public Edge(Edge clone) {
    this.sourceId = clone.getSourceId();
    this.targetId = clone.getTargetId();
    this.error = clone.getError();
  }

  public String getId() {
    return getSourceId() + ">>" + getTargetId();
  }

  public String getSourceId() {
    return sourceId;
  }

  public String getTargetId() {
    return targetId;
  }

  public void setError(String error) {
    this.error = error;
  }

  public String getError() {
    return this.error;
  }

  public boolean hasError() {
    return this.error != null;
  }

  public Node getSource() {
    return source;
  }

  public void setSource(Node source) {
    this.source = source;
  }

  public Node getTarget() {
    return target;
  }

  public void setTarget(Node target) {
    this.target = target;
  }

  public String getGuideType() {
    return guideType;
  }

  public List<Point2D> getGuideValues() {
    return guideValues;
  }

  public void setGuides(String type, List<Point2D> values) {
    this.guideType = type;
    this.guideValues = values;
  }

  public Object toObject() {
    HashMap<String, Object> obj = new HashMap<String, Object>();
    obj.put("source", getSourceId());
    obj.put("target", getTargetId());
    if (hasError()) {
      obj.put("error", error);
    }
    if (guideValues != null) {
      HashMap<String, Object> lineGuidesObj = new HashMap<String, Object>();
      lineGuidesObj.put("type", guideType);

      ArrayList<Object> guides = new ArrayList<Object>();
      for (Point2D point : this.guideValues) {
        HashMap<String, Double> pointObj = new HashMap<String, Double>();
        pointObj.put("x", point.getX());
        pointObj.put("y", point.getY());
        guides.add(pointObj);
      }
      lineGuidesObj.put("values", guides);

      obj.put("guides", lineGuidesObj);
    }

    return obj;
  }

  @SuppressWarnings("unchecked")
  public static Edge fromObject(Object obj) {
    HashMap<String, Object> edgeObj = (HashMap<String, Object>) obj;

    String source = (String) edgeObj.get("source");
    String target = (String) edgeObj.get("target");

    String error = (String) edgeObj.get("error");

    Edge edge = new Edge(source, target);
    edge.setError(error);

    if (edgeObj.containsKey("guides")) {
      Map<String, Object> guideMap =
          (Map<String, Object>) edgeObj.get("guides");
      List<Object> values = (List<Object>) guideMap.get("values");
      String type = (String) guideMap.get("type");

      ArrayList<Point2D> valuePoints = new ArrayList<Point2D>();
      for (Object pointObj : values) {
        Map<String, Double> point = (Map<String, Double>) pointObj;

        Double x = point.get("x");
        Double y = point.get("y");

        valuePoints.add(new Point2D.Double(x, y));
      }

      edge.setGuides(type, valuePoints);
    }

    return edge;
  }

}
