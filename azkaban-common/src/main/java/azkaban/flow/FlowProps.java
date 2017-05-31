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

import azkaban.utils.Props;
import java.util.HashMap;
import java.util.Map;

public class FlowProps {

  private String parentSource;
  private String propSource;
  private Props props = null;

  public FlowProps(final String parentSource, final String propSource) {
    this.parentSource = parentSource;
    this.propSource = propSource;
  }

  public FlowProps(final Props props) {
    this.setProps(props);
  }

  public static FlowProps fromObject(final Object obj) {
    final Map<String, Object> flowMap = (Map<String, Object>) obj;
    final String source = (String) flowMap.get("source");
    final String parentSource = (String) flowMap.get("inherits");

    final FlowProps flowProps = new FlowProps(parentSource, source);
    return flowProps;
  }

  public Props getProps() {
    return this.props;
  }

  public void setProps(final Props props) {
    this.props = props;
    this.parentSource =
        props.getParent() == null ? null : props.getParent().getSource();
    this.propSource = props.getSource();
  }

  public String getSource() {
    return this.propSource;
  }

  public String getInheritedSource() {
    return this.parentSource;
  }

  public Object toObject() {
    final HashMap<String, Object> obj = new HashMap<>();
    obj.put("source", this.propSource);
    if (this.parentSource != null) {
      obj.put("inherits", this.parentSource);
    }
    return obj;
  }
}
