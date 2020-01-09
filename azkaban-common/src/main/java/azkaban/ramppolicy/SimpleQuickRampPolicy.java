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


/**
 * Simple Auto Ramp Policy will be divided to 4 stages
 *  stage 1: 5%
 *  stage 2: 20%
 *  stage 3: 50%
 *  stage 4: 100%
 */
public class SimpleQuickRampPolicy extends SimpleRampPolicy {
  public SimpleQuickRampPolicy(Props sysProps, Props privateProps) {
    super(sysProps, privateProps);
  }

  @Override
  protected int getMaxRampStage() {
    return 4;
  }

  @Override
  protected int getRampStage(ExecutableFlow flow) {
    int percentage = Math.abs(flow.getId().hashCode() % 100);
    if (percentage < 5) {
      return 1;
    } else if (percentage < 25) {
      return 2;
    } else if (percentage < 50) {
      return 3;
    } else {
      return 4;
    }
  }
}
