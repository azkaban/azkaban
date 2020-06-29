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

import java.util.List;

/**
 * JMX API for execution controller to monitor executions.
 */
public interface JmxExecutionControllerMBean {

  @DisplayName("OPERATION: getNumRunningFlows")
  public int getNumRunningFlows();

  @DisplayName("OPERATION: getRunningFlows")
  public String getRunningFlows();

  @DisplayName("OPERATION: getPrimaryExecutorHostPorts")
  public List<String> getPrimaryExecutorHostPorts();

  @DisplayName("OPERATION: getQueuedFlows")
  public String getQueuedFlows();

}
