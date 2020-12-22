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

import static azkaban.Constants.ImageMgmtConstants.IMAGE_VERSION;

import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.dto.BaseDTO.ValidationOnCreate;
import azkaban.imagemgmt.dto.BaseDTO.ValidationOnUpdate;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.dto.ImageVersionDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service layer implementation for processing and delegation of image version APIs. For
 * example API request processing and validation are handled in this layer. Eventually the requests
 * are routed to the DAO layer for data access.
 */
@Singleton
public class ImageVersionServiceImpl implements ImageVersionService {

  private static final Logger log = LoggerFactory.getLogger(ImageVersionServiceImpl.class);

  private final ImageVersionDao imageVersionsDao;
  private final Converter<ImageVersionDTO, ImageVersionDTO, ImageVersion> converter;

  @Inject
  public ImageVersionServiceImpl(final ImageVersionDao imageVersionsDao,
      @Named(IMAGE_VERSION) final Converter converter) {
    this.imageVersionsDao = imageVersionsDao;
    this.converter = converter;
  }

  @Override
  public int createImageVersion(final ImageVersionDTO imageVersion) throws ImageMgmtException {
    // Override the state to NEW during creation of new image version
    imageVersion.setState(State.NEW);
    // Input validation for image version create request
    final List<String> validationErrors = new ArrayList<>();
    if (!ValidatorUtils
        .validateObject(imageVersion, validationErrors, ValidationOnCreate.class)) {
      final String errors = validationErrors.stream().collect(Collectors.joining(","));
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Provide valid "
          + "input for creating image version metadata. Error(s): [%s].", errors));
    }
    return this.imageVersionsDao
        .createImageVersion(this.converter.convertToDataModel(imageVersion));
  }

  @Override
  public List<ImageVersionDTO> findImageVersions(final ImageMetadataRequest imageMetadataRequest)
      throws ImageMgmtException {
    return this.converter.convertToApiResponseDTOs(this.imageVersionsDao
        .findImageVersions(imageMetadataRequest));
  }

  @Override
  public void updateImageVersion(final ImageVersionDTO imageVersion) throws ImageMgmtException {
    // Input validation for image version create request
    final List<String> validationErrors = new ArrayList<>();
    if (!ValidatorUtils.validateObject(imageVersion, validationErrors,
        ValidationOnUpdate.class)) {
      final String errors = validationErrors.stream().collect(Collectors.joining(","));
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Provide valid "
          + "input for creating image version metadata. Error(s): [%s].", errors));
    }
    this.imageVersionsDao.updateImageVersion(this.converter.convertToDataModel(imageVersion));
  }
}
