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

import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import java.io.IOException;
import java.util.List;

/**
 * This service layer interface exposes methods for delegation and processing business logic for
 * image type APIs. For example API request processing and validation are handled in this layer.
 * Eventually the requests are routed to the DAO layer for data access.
 */
public interface ImageTypeService {

  /**
   * Method for finding all ImageTypes.
   *
   * @return List of ImageTypeDTOs
   */
  public List<ImageTypeDTO> getAllImageTypesWithOwnerships();

  /**
   * Method for finding image type by name.
   *
   * @param id - ID of the ImageType to find
   * @return ImageTypeDTO - ImageTypeDTO for the corresponding ImageType id
   */
  public ImageTypeDTO findImageTypeWithOwnershipsById(String id);

  /**
   * Method for finding image type by name.
   *
   * @param imageTypeName - String of the ImageType to find
   * @return ImageTypeDTO - ImageTypeDTO for the corresponding imageTypeName
   * @throws ImageMgmtException
   */

  public ImageTypeDTO findImageTypeWithOwnershipsByName(String imageTypeName)
      throws ImageMgmtException;

  /**
   * Method for processing and delegation of image type creation/registration.
   *
   * @param imageType - DTO which encapsulates  and transfers the create request details
   * @return int - id of the image type metadata record created
   * @throws IOException
   * @throws ImageMgmtException
   */
  public int createImageType(ImageTypeDTO imageType) throws ImageMgmtException;

  /**
   * Method for updating image types.
   *
   * @param imageType - DTO which encapsulates  and transfers the update request details
   * @return int - id of the image type metadata record updated
   * @throws IOException
   * @throws ImageMgmtException
   */
  public int updateImageType(ImageTypeDTO imageType, String updateOp) throws ImageMgmtException;
}
