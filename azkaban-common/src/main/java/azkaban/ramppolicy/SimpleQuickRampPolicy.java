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
import azkaban.utils.Props;


public class SimpleQuickRampPolicy extends AbstractRampPolicy {
  public SimpleQuickRampPolicy(Props sysProps, Props privateProps) {
    super(sysProps, privateProps);
  }

  @Override
  public boolean check(
      ExecutableFlow flow,
      ExecutableRamp executableRamp
  ) {
    if (executableRamp.getState().isPaused()) {
      return false;
    }

    int stage = executableRamp.getState().getRampStage();
    int flowIdHashCode = flow.getId().hashCode();
    return isInRange(stage, flowIdHashCode);
  }

  /**
   * Simple Percentage range will be appled
   * @param stage current ramp stage
   * @param flowIdHashCode hash code of the flow id
   * @return If it is qualified to ramp, return TRUE
   */
  private boolean isInRange(int stage, int flowIdHashCode) {
    int percentage = flowIdHashCode % 4;
    boolean isInRange = false;  // set the safe status
    switch (stage) {
      case 0: // stage 0
        break;
      case 1: // stage 1 = 25%
        isInRange = (percentage < 1);
        break;
      case 2: // stage 2 = 50%
        isInRange = (percentage < 2);
        break;
      default: // stage 3 = 100%
        isInRange = true;
        break;
    }
    return isInRange;
  }
}
