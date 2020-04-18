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
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simple Auto Ramp Policy will be divided to 5 stages
 *  stage 1: 5%
 *  stage 2: 20%
 *  stage 3: 50%
 *  stage 4: 75%
 *  stage 5: 100%
 */
public class SimpleAutoRampPolicy extends SimpleRampPolicy {
  private static final int MAX_RAMP_STAGE = 5;
  private static final ImmutableList<Integer> RAMP_STAGE_RESCALE_TABLE = ImmutableList.<Integer>builder()
      .add(5, 25, 50, 75)
      .build();
  private static final ImmutableList<Integer> AUTO_RAMP_INTERVAL_TABLE = ImmutableList.<Integer>builder()
      .add(1, 2, 3, 4)
      .build();

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAutoRampPolicy.class);


  public SimpleAutoRampPolicy(Props sysProps, Props privateProps) {
    super(sysProps, privateProps);
  }


  @Override
  protected int getMaxRampStage() {
    return MAX_RAMP_STAGE;
  }

  @Override
  protected int getRampStage(ExecutableFlow flow) {
    int percentage = flow.getRampPercentageId();
    for(int i = 0; i < RAMP_STAGE_RESCALE_TABLE.size(); i++) {
      if (percentage < RAMP_STAGE_RESCALE_TABLE.get(i)) {
        return (i + 1);
      }
    }
    return MAX_RAMP_STAGE;
  }

  @Override
  protected void preprocess(ExecutableRamp executableRamp) {
    int escapedDays = TimeUtils.daysEscapedOver(executableRamp.getStartTime());
    int rampStage = executableRamp.getStage();
    int maxStage = getMaxRampStage();

    if (rampStage == 0) {
      // The ramp is still not stated yet. Auto Ramp should not be triggered.
      return;
    }

    try {
      if (escapedDays >= AUTO_RAMP_INTERVAL_TABLE.get(rampStage - 1)) {
        if (rampStage < maxStage) {
          // Ramp up
          executableRamp.rampUp(maxStage);
          LOGGER.info("[AUTO RAMP UP] (rampId = {}, current Stage = {}, new Stage = {}, timeStamp = {}",
              executableRamp.getId(), rampStage, executableRamp.getStage(),
              executableRamp.getLastUpdatedTime());
        }
      }
    } catch (Exception e) {
      LOGGER.error("[AUTO RAMP ERROR] (rampId = {}, ramStage = {}, message = {}",
          executableRamp.getId(), rampStage, e.getMessage());
    }
  }
}
