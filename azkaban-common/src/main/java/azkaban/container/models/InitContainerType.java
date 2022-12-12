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
 *
 */
package azkaban.container.models;

/**
 * This enum defines the init container type like jobtype or dependency. Each InitContainerType
 * is associated with few configs like prefixes to be added to the name of the init container or
 * name of the default volume associated with the init container to distinguish different init
 * categories of init containers.
 */
public enum InitContainerType {
  SECURITY("security-init","security-volume", "PROXY_USER_LIST"),
  JOBTYPE("jobtype-init-", "jobtype-volume-", "JOBTYPE_MOUNT_PATH"),
  DEPENDENCY("dependency-init-", "dependency-volume-", "DEPENDENCY_MOUNT_PATH");

  final String initPrefix;
  final String volumePrefix;
  final String mountPathKey;

  private InitContainerType(final String initPrefix, final String volumePrefix,
      final String mountPathKey) {
    this.initPrefix = initPrefix;
    this.volumePrefix = volumePrefix;
    this.mountPathKey = mountPathKey;
  }
}
