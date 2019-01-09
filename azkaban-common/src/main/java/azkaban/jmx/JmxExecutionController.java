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

package azkaban.jmx;

import azkaban.executor.ExecutionController;
import java.util.ArrayList;
import java.util.List;

/**
 * JMX for execution controller to monitor executions.
 */
public class JmxExecutionController implements JmxExecutionControllerMBean {

  private final ExecutionController controller;

  public JmxExecutionController(final ExecutionController controller) {
    this.controller = controller;
  }

  @Override
  public int getNumRunningFlows() {
    return this.controller.getRunningFlows().size();
  }

  @Override
  public List<String> getPrimaryExecutorHostPorts() {
    return new ArrayList<>(this.controller.getPrimaryServerHosts());
  }

  @Override
  public String getRunningFlows() {
    return this.controller.getRunningFlowIds();
  }

  @Override
  public String getQueuedFlows() {
    return this.controller.getQueuedFlowIds();
  }

}
