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
import azkaban.imagemgmt.models.ImageRampupPlan;
import java.io.IOException;
import java.util.Optional;

/**
 * This service layer interface exposes methods for delegation and processing business logic for
 * image version APIs. For example API request processing and validation are handled in this layer.
 * Eventually the requests are routed to the DAO layer for data access.
 */
public interface ImageRampupService {

  /**
   * Service method to create image rampup plan and rampup. It performs the necessary validation and
   * deletes the rampup creation to the DAO layer.
   *
   * @param imageMetadataRequest
   * @return int - id of the rampup plan
   * @throws IOException
   * @throws ImageMgmtException
   */
  public int createImageRampupPlan(ImageMetadataRequest imageMetadataRequest) throws IOException,
      ImageMgmtException;

  /**
   * Service method to get an active rampup plan for an image type. It fetches the active rampup
   * plan along with rampup with the help of DAO layer method.
   *
   * @param imageTypeName
   * @return Optional<ImageRampupPlan>
   * @throws ImageMgmtException
   */
  public Optional<ImageRampupPlan> getActiveRampupPlan(String imageTypeName)
      throws ImageMgmtException;

  /**
   * Service method to update an active rampup plan for an image type. It fetches the active rampup
   * plan along with rampup with the help of DAO layer method.
   *
   * @param imageMetadataRequest
   * @throws IOException
   * @throws ImageMgmtException
   */
  public void updateImageRampupPlan(ImageMetadataRequest imageMetadataRequest) throws IOException,
      ImageMgmtException;

}
