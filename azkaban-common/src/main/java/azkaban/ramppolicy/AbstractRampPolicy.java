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
 * Abstract Ramp Policy
 */
public abstract class AbstractRampPolicy implements RampPolicy {

  protected volatile Props sysProps;
  protected volatile Props privateProps;


  protected AbstractRampPolicy(final Props sysProps, final Props privateProps) {
    this.sysProps = sysProps;
    this.privateProps = privateProps;
  }

  @Override
  public boolean check(ExecutableFlow flow, ExecutableRamp executableRamp) {
    preprocess(executableRamp);

    if (!executableRamp.getState().isActive() || executableRamp.getState().isPaused() || !executableRamp.getState().isRamping()) {
      return false; // filter out inactive or paused executable ramp flow
    }

    return isRampTestEnabled(flow, executableRamp);
  }

  protected boolean isRampTestEnabled(ExecutableFlow flow, ExecutableRamp executableRamp) {
    return false;
  }

  protected void preprocess(ExecutableRamp executableRamp) {
  }
}
