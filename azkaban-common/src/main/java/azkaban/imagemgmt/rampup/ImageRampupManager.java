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
package azkaban.imagemgmt.rampup;

import azkaban.executor.ExecutableFlow;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageVersionMetadata;
import azkaban.imagemgmt.version.VersionInfo;
import java.util.Map;
import java.util.Set;

/**
 * The interface exposes method for getting version for each image types based on rampup. Dispatch
 * flow container will invoke the exposed methods to get version for the image types.
 */
public interface ImageRampupManager {

  /**
   * Gets the version of all the available active image types based on rampup
   *
   * @return Map<String, VersionInfo>
   * @throws ImageMgmtException
   */
  public Map<String, VersionInfo> getVersionForAllImageTypes(ExecutableFlow flow)
      throws ImageMgmtException;

  /**
   * Fetches the version of all the given image types based on rampup and flow
   *
   * @return Map<String, VersionInfo>
   * @throws ImageMgmtException
   */
  public Map<String, VersionInfo> getVersionByImageTypes(ExecutableFlow flow,
      Set<String> imageTypes)
      throws ImageMgmtException;

  /**
   * Gets the versionMetadata of all the available active image types based on random rampup and
   * active version
   *
   * @return
   * @throws ImageMgmtException
   */
  public Map<String, ImageVersionMetadata> getVersionMetadataForAllImageTypes()
      throws ImageMgmtException;

  /**
   * Gets VersionInfo for the given image type and image version.
   *
   * @param imageType
   * @param imageVersion
   * @return VersionInfo
   * @throws ImageMgmtException
   */
  public VersionInfo getVersionInfo(final String imageType, final String imageVersion)
      throws ImageMgmtException;
}
