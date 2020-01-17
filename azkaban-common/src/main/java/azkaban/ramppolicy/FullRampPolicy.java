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
 * Full Ramp Policy is a Dummy Ramp Policy which does apply any ramp upon the job flow.
 */
public final class FullRampPolicy extends AbstractRampPolicy {

  public FullRampPolicy(Props sysProps, Props privateProps) {
    super(sysProps, privateProps);
  }

  @Override
  protected boolean isRampTestEnabled(ExecutableFlow flow, ExecutableRamp executableRamp) {
    return true;
  }
}
