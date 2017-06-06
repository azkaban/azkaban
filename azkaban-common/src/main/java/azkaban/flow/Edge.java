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

  public Edge(final String fromId, final String toId) {
    this.sourceId = fromId;
    this.targetId = toId;
  }

  public Edge(final Edge clone) {
    this.sourceId = clone.getSourceId();
    this.targetId = clone.getTargetId();
    this.error = clone.getError();
  }

  public static Edge fromObject(final Object obj) {
    final HashMap<String, Object> edgeObj = (HashMap<String, Object>) obj;

    final String source = (String) edgeObj.get("source");
    final String target = (String) edgeObj.get("target");

    final String error = (String) edgeObj.get("error");

    final Edge edge = new Edge(source, target);
    edge.setError(error);

    if (edgeObj.containsKey("guides")) {
      final Map<String, Object> guideMap =
          (Map<String, Object>) edgeObj.get("guides");
      final List<Object> values = (List<Object>) guideMap.get("values");
      final String type = (String) guideMap.get("type");

      final ArrayList<Point2D> valuePoints = new ArrayList<>();
      for (final Object pointObj : values) {
        final Map<String, Double> point = (Map<String, Double>) pointObj;

        final Double x = point.get("x");
        final Double y = point.get("y");

        valuePoints.add(new Point2D.Double(x, y));
      }

      edge.setGuides(type, valuePoints);
    }

    return edge;
  }

  public String getId() {
    return getSourceId() + ">>" + getTargetId();
  }

  public String getSourceId() {
    return this.sourceId;
  }

  public String getTargetId() {
    return this.targetId;
  }

  public String getError() {
    return this.error;
  }

  public void setError(final String error) {
    this.error = error;
  }

  public boolean hasError() {
    return this.error != null;
  }

  public Node getSource() {
    return this.source;
  }

  public void setSource(final Node source) {
    this.source = source;
  }

  public Node getTarget() {
    return this.target;
  }

  public void setTarget(final Node target) {
    this.target = target;
  }

  public String getGuideType() {
    return this.guideType;
  }

  public List<Point2D> getGuideValues() {
    return this.guideValues;
  }

  public void setGuides(final String type, final List<Point2D> values) {
    this.guideType = type;
    this.guideValues = values;
  }

  public Object toObject() {
    final HashMap<String, Object> obj = new HashMap<>();
    obj.put("source", getSourceId());
    obj.put("target", getTargetId());
    if (hasError()) {
      obj.put("error", this.error);
    }
    if (this.guideValues != null) {
      final HashMap<String, Object> lineGuidesObj = new HashMap<>();
      lineGuidesObj.put("type", this.guideType);

      final ArrayList<Object> guides = new ArrayList<>();
      for (final Point2D point : this.guideValues) {
        final HashMap<String, Double> pointObj = new HashMap<>();
        pointObj.put("x", point.getX());
        pointObj.put("y", point.getY());
        guides.add(pointObj);
      }
      lineGuidesObj.put("values", guides);

      obj.put("guides", lineGuidesObj);
    }

    return obj;
  }

}
