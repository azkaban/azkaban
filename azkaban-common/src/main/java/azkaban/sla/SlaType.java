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

package azkaban.sla;

/**
 * SLA type -- if SLA is for a flow or job that has succeeded or finished.
 */
public enum SlaType {
  FLOW_FINISH("FlowFinish", ComponentType.FLOW, StatusType.FINISH),
  FLOW_SUCCEED("FlowSucceed", ComponentType.FLOW, StatusType.SUCCEED),
  JOB_FINISH("JobFinish", ComponentType.JOB, StatusType.FINISH),
  JOB_SUCCEED("JobSucceed", ComponentType.JOB, StatusType.SUCCEED);

  /**
   * The component the SLA is for: a flow or job.
   */
  public enum ComponentType {
    FLOW,
    JOB
  }

  /**
   * The status the SLA is for: finish or succeed.
   */
  public enum StatusType {
    FINISH,
    SUCCEED
  }

  final private String name;
  final private ComponentType component;
  final private StatusType status;

  /**
   * Constructor.
   *
   * @param name the SLA type name.
   * @param component The component the SLA is for, either flow or job.
   * @param status the status the SLA is for, either succeed or finish.
   */
  SlaType(String name, ComponentType component, StatusType status) {
    this.name = name;
    this.component = component;
    this.status = status;
  }

  public String getName() {
    return name;
  }

  public ComponentType getComponent() {
    return component;
  }

  public StatusType getStatus() {
    return status;
  }
}
