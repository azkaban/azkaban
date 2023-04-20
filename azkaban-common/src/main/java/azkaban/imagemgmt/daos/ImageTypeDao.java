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

import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageType;
import java.util.List;
import java.util.Optional;

/**
 * Data access object (DAO) for accessing image types. This interface defines method such as create,
 * get image types etc.
 */
public interface ImageTypeDao {

  /**
   * Create/register a new image type metadata.
   *
   * @param imageType - ImageType to be created
   * @return int - Id representing image type metadata
   */
  public int createImageType(ImageType imageType) throws ImageMgmtException;

  /**
   * Gets an image type metadata based on image type name.
   *
   * @param name - Name of the image type
   * @return Optional<ImageType> - Optional representing ImageType
   */
  public Optional<ImageType> getImageTypeByName(String name) throws ImageMgmtException;

  /**
   * Get all image types
   *
   * @return List<ImageType>
   * @throws ImageMgmtException
   */
  public List<ImageType> getAllImageTypes() throws ImageMgmtException;

  /**
   * Get image type with ownership metadata based on specified image type name.
   *
   * @param name
   * @return Optional<ImageType>
   * @throws ImageMgmtException
   */
  public Optional<ImageType> getImageTypeWithOwnershipsByName(String name)
      throws ImageMgmtException;

  /**
   * Get all image types with ownership metadata.
   *
   * @return List<ImageType>
   * @throws ImageMgmtException
   */
  public List<ImageType> getAllImageTypesWithOwnerships() throws ImageMgmtException;

  /**
   * Gets ownership metadata based on image type name.
   *
   * @param imageTypeName
   * @return List<ImageOwnership>
   */
  public List<ImageOwnership> getImageTypeOwnership(final String imageTypeName) throws ImageMgmtException;

  /**
   * Gets Image Type metadata with Ownerships based on String Image Type id.
   *
   * @param id
   * @return ImageType
   */
  public ImageType getImageTypeWithOwnershipsById(String id);

  /**
   * Add an owner for a given image type.
   *
   * @param imageType - ImageType to be updated
   * @return int - Id representing image type metadata
   */
  public int addImageTypeOwner(ImageType imageType) throws ImageMgmtException;

  /**
   * Remove an owner for a given image type.
   *
   * @param imageType - ImageType to be updated
   * @return int - Id representing image type metadata
   */
  public int removeImageTypeOwner(ImageType imageType) throws ImageMgmtException;

}
