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
import azkaban.utils.TimeUtils;


/**
 * Simple Auto Ramp Policy will be divided to 4 stages
 *  stage 1: 5%
 *  stage 2: 20%
 *  stage 3: 50%
 *  stage 4: 100%
 */
public class SimpleAutoRampPolicy extends AbstractRampPolicy {
  private static int RAMP_STAGE_MAX = 4;
  private static int ONE_DAY = 86400;
  public SimpleAutoRampPolicy(Props sysProps, Props privateProps) {
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

    updateForRampUp(executableRamp, RAMP_STAGE_MAX, ONE_DAY);

    int stage = executableRamp.getState().getRampStage();
    int flowIdHashCode = flow.getId().hashCode();
    return isInRange(stage, flowIdHashCode);
  }

  /**
   * Auto Ramp up to next stage when there is no more change in one day
   * @param executableRamp executableRamp
   */
  protected void updateForRampUp(ExecutableRamp executableRamp, final int maxRampStage, final int interval) {
    if (TimeUtils.timeEscapedOver(executableRamp.getState().getLastUpdatedTime(), interval)) {
      int rampStage = executableRamp.getState().getRampStage();
      if (rampStage < maxRampStage) {
        executableRamp.getState().setRampStage(rampStage + 1);
        executableRamp.getState().setLastUpdatedTime(System.currentTimeMillis());
      } else {
        executableRamp.getState().setEndTime(System.currentTimeMillis());
      }
    }
  }

  /**
   * Simple Percentage range will be applied
   * @param stage current stage
   * @param flowIdHashCode hash code of flow_Id
   * @return
   */
  private boolean isInRange(int stage, int flowIdHashCode) {
    int percentage = Math.abs(flowIdHashCode % 100);
    boolean isInRange = false;  // set the safe status
    switch (stage) {
      case 0: // stage 0
        break;
      case 1: // stage 1 = 5%
        isInRange = (percentage <= 5);
        break;
      case 2: // stage 2 = 20%
        isInRange = (percentage <= 20);
        break;
      case 3: // stage 3 = 50%
        isInRange = (percentage <= 50);
        break;
      default: // stage 4 = 100%
        isInRange = true;
        break;
    }
    return isInRange;
  }
}
