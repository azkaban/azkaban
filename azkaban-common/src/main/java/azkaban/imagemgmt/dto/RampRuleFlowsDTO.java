/*
 * Copyright 2022 LinkedIn Corp.
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
package azkaban.imagemgmt.dto;

import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.apache.htrace.shaded.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class represents adding flows to ramp rule request.
 */
public class RampRuleFlowsDTO extends BaseDTO {
  // the name of ramp rule
  @JsonProperty("ruleName")
  @NotNull
  public String ruleName;
  // flowIds associated with rule
  @JsonProperty("flowIds")
  @NotNull
  @NotEmpty
  public List<ProjectFlow> flowIds;

  public String getRuleName() {
    return ruleName;
  }

  public void setRuleName(String ruleName) {
    this.ruleName = ruleName;
  }

  public List<ProjectFlow> getFlowIds() {
    return flowIds;
  }

  public void setFlowIds(List<ProjectFlow> flowIds) {
    this.flowIds = flowIds;
  }

  public static class ProjectFlow {
    @JsonProperty("projectName")
    public String projectName;
    @JsonProperty("flowName")
    public String flowName;

    @Override
    public String toString() {
      return String.join(".", projectName, flowName);
    }
  }
}
