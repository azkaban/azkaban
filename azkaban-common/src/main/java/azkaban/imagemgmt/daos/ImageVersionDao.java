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
package azkaban.imagemgmt.daos;

import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Data access object (DAO) for accessing image version metadata. This interface defines method such
 * as create, get image version metadata etc.
 */
public interface ImageVersionDao {

  /**
   * Creates image version metadata for an image type.
   *
   * @param imageVersionRequest
   * @return int - id of the image version
   */
  public int createImageVersion(ImageVersion imageVersionRequest);

  /**
   * Method to find image versions based on image metadata such as image type, image version,
   * version state etc.
   *
   * @param imageMetadataRequest
   * @return List
   * @throws ImageMgmtException
   */
  public List<ImageVersion> findImageVersions(ImageMetadataRequest imageMetadataRequest)
      throws ImageMgmtException;

  /**
   * Method to get image version based on image type, image version and version state
   *
   * @param imageTypeName
   * @param imageVersion
   * @param versionState
   * @return Optional<ImageVersion>
   * @throws ImageMgmtException
   */
  public Optional<ImageVersion> getImageVersion(String imageTypeName, String imageVersion,
      State versionState) throws ImageMgmtException;

  /**
   * Returns active image versions for the given image types.
   *
   * @param imageTypes
   * @return List<ImageVersion>
   * @throws ImageMgmtException
   */
  public List<ImageVersion> getActiveVersionByImageTypes(Set<String> imageTypes)
      throws ImageMgmtException;

  /**
   * Returns latest non active image versions for the given image types.
   *
   * @param imageTypes
   * @return List<ImageVersion>
   * @throws ImageMgmtException
   */
  public List<ImageVersion> getLatestNonActiveVersionByImageTypes(final Set<String> imageTypes)
      throws ImageMgmtException;

  /**
   * Updates image version metadata.
   *
   * @param imageVersionRequest
   * @throws ImageMgmtException
   */
  public void updateImageVersion(ImageVersion imageVersionRequest)
      throws ImageMgmtException;

  /**
   * Gets image version based on image type and version.
   *
   * @param imageTypeName
   * @param imageVersion
   * @return Optional<ImageVersion>
   * @throws ImageMgmtException
   */
  public Optional<ImageVersion> getImageVersion(final String imageTypeName,
      final String imageVersion) throws ImageMgmtException;
}
