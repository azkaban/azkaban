/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.executor.container;

import azkaban.executor.ExecutorManagerException;
import java.time.Duration;
import java.util.Set;

public interface ContainerizedImpl {
  void createContainer(final int executionId) throws ExecutorManagerException;
  void deleteContainer(final int executionId) throws ExecutorManagerException;
  Set<Integer> getContainersByDuration(final Duration containerDuration) throws ExecutorManagerException;

  /**
   * Set up ramp up rate for VPA feature. For example, if rampUp is 10%, 10% of flows will be
   * guided by VPA to determine flow resource limits.
   *
   * @param rampUp VPA rampUp rate: e.g. 0, 10, 20, 100
   */
  void setVPARampUp(int rampUp);

  /**
   * Get the current VPA ramp up rate.
   *
   * @return current VPA ramp up rate
   */
  int getVPARampUp();

  /**
   * Globally enable or disable VPA feature. It takes precedence over the VPA ramp up rate.
   *
   * @param enabled VPA enabled status
   */
  void setVPAEnabled(boolean enabled);

  /**
   * Get the current VPA enabled status.
   *
   * @return VPA enabled status
   */
  boolean getVPAEnabled();
}
