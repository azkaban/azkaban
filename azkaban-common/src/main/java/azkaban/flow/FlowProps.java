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

import java.util.HashMap;
import java.util.Map;

import azkaban.utils.Props;

public class FlowProps {
  private String parentSource;
  private String propSource;
  private Props props = null;

  public FlowProps(String parentSource, String propSource) {
    this.parentSource = parentSource;
    this.propSource = propSource;
  }

  public FlowProps(Props props) {
    this.setProps(props);
  }

  public Props getProps() {
    return props;
  }

  public void setProps(Props props) {
    this.props = props;
    this.parentSource =
        props.getParent() == null ? null : props.getParent().getSource();
    this.propSource = props.getSource();
  }

  public String getSource() {
    return propSource;
  }

  public String getInheritedSource() {
    return parentSource;
  }

  public Object toObject() {
    HashMap<String, Object> obj = new HashMap<String, Object>();
    obj.put("source", propSource);
    if (parentSource != null) {
      obj.put("inherits", parentSource);
    }
    return obj;
  }

  @SuppressWarnings("unchecked")
  public static FlowProps fromObject(Object obj) {
    Map<String, Object> flowMap = (Map<String, Object>) obj;
    String source = (String) flowMap.get("source");
    String parentSource = (String) flowMap.get("inherits");

    FlowProps flowProps = new FlowProps(parentSource, source);
    return flowProps;
  }
}
