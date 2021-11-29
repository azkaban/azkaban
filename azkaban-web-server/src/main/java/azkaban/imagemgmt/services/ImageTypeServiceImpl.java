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

import static azkaban.Constants.ImageMgmtConstants.IMAGE_TYPE;

import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.dto.ImageOwnershipDTO;
import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageOwnership.Role;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.models.ImageType.Deployable;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service layer implementation for processing and delegation of image type APIs. For example
 * API request processing and validation are handled in this layer. Eventually the requests are
 * routed to the DAO layer for data access.
 */
@Singleton
public class ImageTypeServiceImpl implements ImageTypeService {

  private static final Logger log = LoggerFactory.getLogger(ImageTypeServiceImpl.class);

  private final ImageTypeDao imageTypeDao;
  private final Converter<ImageTypeDTO, ImageTypeDTO, ImageType> converter;

  @Inject
  public ImageTypeServiceImpl(final ImageTypeDao imageTypeDao,
      @Named(IMAGE_TYPE) final Converter converter) {
    this.imageTypeDao = imageTypeDao;
    this.converter = converter;
  }

  @Override
  public int createImageType(final ImageTypeDTO imageType) throws ImageMgmtException {
    // By default always image.
    if (imageType.getDeployable() == null) {
      imageType.setDeployable(Deployable.IMAGE);
    }
    // Input validation for image type create request
    final List<String> validationErrors = new ArrayList<>();
    if (!ValidatorUtils.validateObject(imageType, validationErrors)) {
      final String errors = validationErrors.stream().collect(Collectors.joining(","));
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Provide valid "
          + "input for creating image type metadata. Error(s): [%s].", errors));
    }
    // Validate ownership metadata
    validateOwnership(imageType);
    return this.imageTypeDao.createImageType(this.converter.convertToDataModel(imageType));
  }

  /**
   * Validate image type ownership metadata.
   *
   * @param imageType
   * @return boolean
   * @throws ImageMgmtValidationException
   */
  private boolean validateOwnership(final ImageTypeDTO imageType)
      throws ImageMgmtValidationException {
    // Check if ownership record exists
    if (CollectionUtils.isEmpty(imageType.getOwnerships())
        || imageType.getOwnerships().size() < 2) {
      log.error("Please specify at least two owners for the image type: {} ", imageType.getName());
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Please specify"
          + " at least two owners for the image type: %s. ", imageType.getName()));
    }
    // Check if one of the owner is admin
    if (!hasAdminRole(imageType)) {
      log.error("Please specify at least one ADMIN owner for image type: {} ", imageType.getName());
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Please specify"
          + " at least one ADMIN owner for image type: %s. ", imageType.getName()));
    }
    // Ownership metadata must not contain duplicate owners.
    if (hasDuplicateOwner(imageType)) {
      log.error("The ownership data contains duplicate owners.");
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST,
          "The ownership data contains duplicate owners.");
    }
    return true;
  }

  /**
   * Validate if ownership information contains ADMIN role.
   *
   * @param imageType
   * @return boolean
   */
  private boolean hasAdminRole(final ImageTypeDTO imageType) {
    boolean hasAdminRole = false;
    for (final ImageOwnershipDTO imageOwnership : imageType.getOwnerships()) {
      if (Role.ADMIN.equals(imageOwnership.getRole())) {
        hasAdminRole = true;
      }
    }
    return hasAdminRole;
  }

  /**
   * Validate if ownership information contains duplicate owners.
   *
   * @param imageType
   * @return boolean
   */
  private boolean hasDuplicateOwner(final ImageTypeDTO imageType) {
    final Set<String> owners = new HashSet<>();
    for (final ImageOwnershipDTO imageOwnership : imageType.getOwnerships()) {
      if (owners.contains(imageOwnership.getOwner())) {
        return true;
      } else {
        owners.add(imageOwnership.getOwner());
      }
    }
    return false;
  }
}
