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
package azkaban.imagemgmt.services;

import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageVersion;
import java.io.IOException;
import java.util.List;

/**
 * This service layer interface exposes methods for delegation and processing business logic for
 * image version APIs. For example API request processing and validation are handled in this layer.
 * Eventually the requests are routed to the DAO layer for data access.
 */
public interface ImageVersionService {

  /**
   * Create a new version of an image type using given image metadata.
   *
   * @param imageMetadataRequest
   * @return int
   * @throws IOException
   * @throws ImageMgmtException
   */
  public int createImageVersion(ImageMetadataRequest imageMetadataRequest) throws IOException,
      ImageMgmtException;

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

}
