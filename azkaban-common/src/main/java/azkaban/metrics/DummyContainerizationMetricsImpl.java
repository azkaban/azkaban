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
package azkaban.metrics;

import azkaban.utils.Props;
import org.apache.log4j.Logger;

/**
 * No-op implementation of {@link ContainerizationMetrics} used when dispatch method is not
 * CONTAINERIZED or during unit test
 */
public class DummyContainerizationMetricsImpl implements ContainerizationMetrics {
  private static final Logger logger = Logger.getLogger(ContainerizationMetricsImpl.class);

  @Override
  public void setUp() {
    logger.debug("No metrics set up for containerized execution");
  }

  @Override
  public void startReporting(Props props) {
  }

  @Override
  public void markPodCompleted() {
  }

  @Override
  public void markPodRequested() {
  }

  @Override
  public void markPodScheduled() {
  }

  @Override
  public void markInitContainerRunning() {
  }

  @Override
  public void markAppContainerStarting() {
  }

  @Override
  public void markPodReady() {
  }

  @Override
  public void markPodInitFailure() {
  }

  @Override
  public void markPodAppFailure() {
  }

  @Override
  public void addTimeToDispatch(long time) {
  }

  @Override
  public void markFlowSubmitToExecutor() {

  }

  @Override
  public void markFlowSubmitToContainer() {

  }
}
