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

package azkaban.utils;

import azkaban.spi.Dependency;
import org.apache.commons.codec.binary.Hex;

import static azkaban.utils.ThinArchiveUtils.convertIvyCoordinateToPath;


public class StorageUtils {
  public static String getTargetProjectFilename(final int projectId, byte[] hash) {
    return String.format("%s-%s.zip",
        String.valueOf(projectId),
        new String(Hex.encodeHex(hash))
    );
  }

  public static String getTargetDependencyPath(final Dependency dep) {
    // For simplicity, we will set the path to store dependencies the same as the in the URL used
    // for fetching the dependencies. It will follow the pattern:
    // samsa/samsa-api/0.6.0/samsa-api-0.6.0.jar
    return convertIvyCoordinateToPath(dep);
  }
}
