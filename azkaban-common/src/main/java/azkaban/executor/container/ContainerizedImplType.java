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

/**
 * This enum represents implementation for Containerization. For now, we are adding
 * implementation for Kubernetes but in future, execution can be dispatched on any other
 * containerized infrastructure.
 */
public enum ContainerizedImplType {
  KUBERNETES(KubernetesContainerizedImpl.class);

  private final Class implClass;

  ContainerizedImplType(final Class implClass) {
    this.implClass = implClass;
  }

  public Class getImplClass() {
    return this.implClass;
  }
}
