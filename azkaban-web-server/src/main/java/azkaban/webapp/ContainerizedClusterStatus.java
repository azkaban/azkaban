/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.webapp;

import azkaban.imagemgmt.dto.ImageVersionMetadataResponseDTO;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * This POJO is used by GSON library to create a status JSON object. This class represents status
 * for containerized cluster.
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class ContainerizedClusterStatus implements Status {

  private final String version;
  private final String pid;
  private final String installationPath;
  private final long usedMemory, xmx;
  private final boolean isDatabaseUp;
  private final Map<String, ImageVersionMetadataResponseDTO> imageTypeVersionMap;

  ContainerizedClusterStatus(final String version,
      final String pid,
      final String installationPath,
      final long usedMemory,
      final long xmx,
      final boolean isDatabaseUp,
      final Map<String, ImageVersionMetadataResponseDTO> imageTypeVersionMap) {
    this.version = version;
    this.pid = pid;
    this.installationPath = installationPath;
    this.usedMemory = usedMemory;
    this.xmx = xmx;
    this.isDatabaseUp = isDatabaseUp;
    this.imageTypeVersionMap = ImmutableMap.copyOf(imageTypeVersionMap);
  }
}
