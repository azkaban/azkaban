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

import static azkaban.Constants.ImageMgmtConstants.IMAGE_RAMPUP_PLAN;

import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.daos.ImageRampupDao;
import azkaban.imagemgmt.dto.ImageRampupDTO;
import azkaban.imagemgmt.dto.ImageRampupPlanRequestDTO;
import azkaban.imagemgmt.dto.ImageRampupPlanResponseDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageRampup.StabilityTag;
import azkaban.imagemgmt.models.ImageRampupPlan;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service layer implementation for processing and delegation of image rampup APIs. For example
 * API request processing and validation are handled in this layer. Eventually the requests are
 * routed to the DAO layer for data access.
 */
@Singleton
public class ImageRampupServiceImpl implements ImageRampupService {

  private static final Logger log = LoggerFactory.getLogger(ImageRampupServiceImpl.class);

  private final ImageRampupDao imageRampupDao;
  private final Converter<ImageRampupPlanRequestDTO, ImageRampupPlanResponseDTO, ImageRampupPlan> converter;

  @Inject
  public ImageRampupServiceImpl(final ImageRampupDao imageRampupDao,
      @Named(IMAGE_RAMPUP_PLAN) final Converter converter) {
    this.imageRampupDao = imageRampupDao;
    this.converter = converter;
  }

  @Override
  public int createImageRampupPlan(final ImageRampupPlanRequestDTO imageRampupPlanRequest)
      throws ImageMgmtException {
    // input validation for ImageRampupPlanRequest
    final List<String> validationErrors = new ArrayList<>();
    if (!ValidatorUtils.validateObject(imageRampupPlanRequest, validationErrors)) {
      final String errors = validationErrors.stream().collect(Collectors.joining(","));
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Provide valid "
          + "input for creating image rampup plan. Error(s): [%s]", errors));
    }
    // Validate image rampup plan request
    vaidateRampup(imageRampupPlanRequest);

    // Invoke DAO method to create rampup plan and rampup details
    return this.imageRampupDao
        .createImageRampupPlan(this.converter.convertToDataModel(imageRampupPlanRequest));
  }

  @Override
  public Optional<ImageRampupPlanResponseDTO> getActiveRampupPlan(final String imageTypeName)
      throws ImageMgmtException {
    final Optional<ImageRampupPlan> optionalImageRampupPlan =
        this.imageRampupDao.getActiveImageRampupPlan(imageTypeName, true);
    if (optionalImageRampupPlan.isPresent()) {
      return Optional.of(this.converter.convertToApiResponseDTO(optionalImageRampupPlan.get()));
    }
    return Optional.empty();
  }

  @Override
  public void updateImageRampupPlan(final ImageRampupPlanRequestDTO imageRampupPlanRequest) throws
      ImageMgmtException {

    // input validation for image version create request
    final List<String> validationErrors = new ArrayList<>();
    if (!ValidatorUtils.validateObject(imageRampupPlanRequest, validationErrors)) {
      final String errors = validationErrors.stream().collect(Collectors.joining(","));
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Provide valid "
          + "input for updating image rampup plan. Error(s): [%s]", errors));
    }
    // Validate rampup details and set the modified by user
    if (!CollectionUtils.isEmpty(imageRampupPlanRequest.getImageRampups())) {
      vaidateRampup(imageRampupPlanRequest);
    }
    this.imageRampupDao.updateImageRampupPlan(this.converter.convertToDataModel(imageRampupPlanRequest));
  }

  /**
   * Validate input provided as part of rampup request. Here are the validations - 1. Total rampup
   * percentage must add upto 100. 2. Rampup details must not have duplicate image versions. 3. If a
   * specific version is marked as UNSTABLE in the rampup, the corresponding rampup percentage must
   * be zero.
   *
   * @param imageRampupPlanRequest
   * @return boolean
   * @throws ImageMgmtValidationException
   */
  private boolean vaidateRampup(final ImageRampupPlanRequestDTO imageRampupPlanRequest)
      throws ImageMgmtValidationException {
    final List<ImageRampupDTO> imageRampupRequests = imageRampupPlanRequest.getImageRampups();
    log.info("vaidateRampup imageRampupRequests: {} ", imageRampupRequests);
    if (CollectionUtils.isEmpty(imageRampupRequests)) {
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, "Missing rampup details");
    }
    // Check for total rampup percentage
    int totalRampupPercentage = 0;
    for (final ImageRampupDTO imageRampupRequest : imageRampupRequests) {
      totalRampupPercentage += imageRampupRequest.getRampupPercentage();
    }
    if (totalRampupPercentage != 100) {
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, "Total rampup percentage for "
          + "all the version must be 100");
    }

    // Check for duplicate image version
    final Set<String> versions = new HashSet<>();
    for (final ImageRampupDTO imageRampupRequest : imageRampupRequests) {
      if (!versions.contains(imageRampupRequest.getImageVersion())) {
        versions.add(imageRampupRequest.getImageVersion());
      } else {
        throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Duplicate "
                + "image version: %s.",
            imageRampupRequest.getImageVersion()));
      }
    }

    // check for stability tag and ramp up percentage
    for (final ImageRampupDTO imageRampupRequest : imageRampupRequests) {
      if (StabilityTag.UNSTABLE.equals(imageRampupRequest.getStabilityTag())
          && imageRampupRequest.getRampupPercentage() != 0) {
        throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("The image "
                + "version: %s is marked as UNSTABLE in the input and hence the rampup percentage "
                + "must be 0.",
            imageRampupRequest.getImageVersion()));
      }
    }
    return true;
  }
}
