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

import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.utils.Props;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: Add implementation for integration with Kubernetes once it is tested on cluster.
 */
public class KubernetesContainerizedImpl implements ContainerizedImpl {
  private final Props azkProps;
  private final ExecutorLoader executorLoader;
  private static final Logger logger = LoggerFactory.getLogger(KubernetesContainerizedImpl.class);

  @Inject
  KubernetesContainerizedImpl(final Props azkProps, final ExecutorLoader executorLoader) throws ExecutorManagerException {
    this.azkProps = azkProps;
    this.executorLoader = executorLoader;
  }

  @Override
  public void createContainer(final int executionId) throws ExecutorManagerException {
    logger.info("Container is created");
  }

  @Override
  public void deleteContainer(final int executionId) throws ExecutorManagerException {
    logger.info("Container is deleted");
  }
}
