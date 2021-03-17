/*
 * Copyright 2021 LinkedIn Corp.
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

import azkaban.Constants.ContainerizedDispatchManagerProperties;
import azkaban.DispatchMethod;
import azkaban.executor.ExecutableFlow;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for determining {@link DispatchMethod} based on rampUp.
 */
public class ContainerRampUpCriteria {

  private int rampUp;
  private static final Logger logger = LoggerFactory.getLogger(ContainerRampUpCriteria.class);

  public ContainerRampUpCriteria(final Props azkProps) {
    int rampUp = azkProps.getInt(ContainerizedDispatchManagerProperties.CONTAINERIZED_RAMPUP, 100);
    if (rampUp > 100 || rampUp < 0) {
      String errorMessage = "RampUp must be an integer between [0, 100]: " + rampUp;
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    } else {
      this.rampUp = rampUp;
    }
  }

  /**
   * Return DispatchMethod based on the rampUp percentage and random selection
   */
  public DispatchMethod getDispatchMethod() {
    if (this.rampUp == 0) {
      return DispatchMethod.POLL;
    } else if (this.rampUp == 100) {
      return DispatchMethod.CONTAINERIZED;
    }
    ThreadLocalRandom rand = ThreadLocalRandom.current();
    int randomInt = rand.nextInt(100);
    if (randomInt < this.rampUp) {
      return DispatchMethod.CONTAINERIZED;
    } else {
      return DispatchMethod.POLL;
    }
  }

  /**
   * Return DispatchMethod based on the rampUp percentage and deterministic selection
   * based on the flow name
   */
  public DispatchMethod getDispatchMethod(final ExecutableFlow flow) {
    if (this.rampUp == 0) {
      return DispatchMethod.POLL;
    } else if (this.rampUp == 100) {
      return DispatchMethod.CONTAINERIZED;
    }
    int flowNameHashValMapping = ContainerImplUtils.getFlowNameHashValMapping(flow);

    if (flowNameHashValMapping <= this.rampUp) {
      return DispatchMethod.CONTAINERIZED;
    } else {
      return DispatchMethod.POLL;
    }
  }

  @VisibleForTesting
  public void setRampUp(int rampUp) {
    this.rampUp = rampUp;
  }

  @VisibleForTesting
  public int getRampUp() {
    return this.rampUp;
  }
}
