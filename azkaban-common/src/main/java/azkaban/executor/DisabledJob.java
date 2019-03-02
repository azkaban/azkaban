/*
 * Copyright 2019 LinkedIn Corp.
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Disabled job, or disabled sub-jobs for an embedded flow.
 */
public class DisabledJob {

  static final private String SUBFLOW_ID_KEY = "id";
  static final private String SUBFLOW_CHILDREN_KEY = "children";

  final private String name; // name of the disabled job, or embedded flow
  final private ImmutableList<DisabledJob> children; // disabled sub-jobs for an embedded flow

  /**
   * Constructor.
   *
   * @param name the name of the disabled job or embedded flow.
   * @param children if an embedded flow, the disabled jobs for the embedded flow. null if it is
   * a job.
   */
  public DisabledJob(String name, List<DisabledJob> children) {
    this.name = Preconditions.checkNotNull(name, "name is null");
    if (children == null) {
      this.children = null;
    } else {
      this.children = ImmutableList.copyOf(children);
    }
  }


  /**
   * Constructor.
   *
   * @param name the name of the disabled job.
   */
  public DisabledJob(String name) {
    this(name, null);
  }

  public String getName() {
    return this.name;
  }

  public List<DisabledJob> getChildren() {
    return this.children;
  }

  /** @return True if this is an embedded flow, false if it is a single job. */
  public boolean isEmbeddedFlow() {
    return (this.children != null);
  }

  /** @return The original Object/JSON format, for {@link azkaban.sla.SlaOptionDeprecated}. */
  public Object toDeprecatedObject() {
    if (this.children != null) {
      Object childrenObj = this.children
          .stream().map(DisabledJob::toDeprecatedObject).collect(Collectors.toList());
      return ImmutableMap.of(SUBFLOW_ID_KEY, name, SUBFLOW_CHILDREN_KEY, childrenObj);

    } else {
      return this.name;
    }
  }

  /**
   * Convert a list of DisabledJobs to the original Object/JSON format, for
   * {@link azkaban.sla.SlaOptionDeprecated}.
   *
   * @param disabledJobs list of disabled jobs to convert.
   * @return List of original Object/JSON format for the jobs.
   */
  static public List<Object> toDeprecatedObjectList(List<DisabledJob> disabledJobs) {
    return disabledJobs.stream().map( x -> {
          if (x == null) {
            return null;
          }
          else {
            return x.toDeprecatedObject();
          }
    }
    ).collect(Collectors.toList());
  }

  /**
   * Create a DisabledJob from the original Object/JSON format, for
   * {@link azkaban.sla.SlaOptionDeprecated}.
   *
   * @param obj the Object/JSON (in {@link azkaban.sla.SlaOptionDeprecated} format) value.
   * @return the disabled job.
   */
  static public DisabledJob fromDeprecatedObject(Object obj) {
    if (obj == null) {
      return null;
    }
    if (obj instanceof String) {
      return new DisabledJob((String)obj);
    } else if (obj instanceof Map) {
      Map<String, Object> map = (Map<String, Object>)obj;
      String name = (String)map.get(SUBFLOW_ID_KEY);
      List<DisabledJob> childJobs = fromDeprecatedObjectList((List<Object>)map.get
          (SUBFLOW_CHILDREN_KEY));
      if (name != null && childJobs != null) {
        return new DisabledJob(name, childJobs);
      }
    }
    return null;
  }

  /**
   * Construct a list of disabled jobs from a list of original Object/JSON formats, for
   *
   *    * {@link azkaban.sla.SlaOptionDeprecated}.
   * @param objList the list of original Object/JSON formats representing the disabled jobs.
   * @return the list of disabled jobs.
   */
  static public List<DisabledJob> fromDeprecatedObjectList(List<Object> objList) {
    if (objList == null) {
      return null;
    }
    return objList.stream().map(x -> {
        if (x == null) {
          return null;
        } else {
          return fromDeprecatedObject(x);
        }
    }).collect(Collectors.toList());
  }
}
