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
package azkaban.ramppolicy;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableRamp;


/**
 * Raw Ramp Policy interface.
 *
 * A ramp policy is a defined strategy for rampping service.
 */
public interface RampPolicy {

  /**
   * Run the ramp policy. In general this method can only be apply once. Must either succeed or throw an
   *
   * @param flow executable flow
   * @param executableRamp executed ramp
   * @return if the flow is qualified for ramp, returns TRUE.
   * @throws Exception
   */
  boolean check(
      ExecutableFlow flow,
      ExecutableRamp executableRamp
  ) throws Exception;
}
