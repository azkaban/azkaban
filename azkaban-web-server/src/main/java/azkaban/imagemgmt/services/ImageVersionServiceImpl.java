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

import static azkaban.Constants.ImageMgmtConstants.ID_KEY;

import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exeception.ErrorCode;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.models.ImageVersionRequest;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
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
  private final ConverterUtils converterUtils;

  @Inject
  public ImageVersionServiceImpl(ImageVersionDao imageVersionsDao,
      ConverterUtils converterUtils) {
    this.imageVersionsDao = imageVersionsDao;
    this.converterUtils = converterUtils;
  }

  @Override
  public int createImageVersion(ImageMetadataRequest imageMetadataRequest)
      throws IOException, ImageMgmtException {
    // Convert input json payload to image version object
    ImageVersion imageVersion = converterUtils.convertToModel(imageMetadataRequest.getJsonPayload(),
        ImageVersion.class);
    // Set the user who invoked the API
    imageVersion.setCreatedBy(imageMetadataRequest.getUser());
    imageVersion.setModifiedBy(imageMetadataRequest.getUser());
    // Override the state to NEW during creation of new image version
    imageVersion.setState(State.NEW);
    // Input validation for image version create request
    final List<String> validationErrors = new ArrayList<>();
    if (!ValidatorUtils.validateObject(imageVersion, validationErrors)) {
      String errors = validationErrors.stream().collect(Collectors.joining(","));
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Provide valid "
          + "input for creating image version metadata. Error(s): [%s].", errors));
    }
    return imageVersionsDao.createImageVersion(imageVersion);
  }

  @Override
  public List<ImageVersion> findImageVersions(ImageMetadataRequest imageMetadataRequest)
      throws ImageMgmtException {
    return imageVersionsDao.findImageVersions(imageMetadataRequest);
  }

  @Override
  public void updateImageVersion(ImageMetadataRequest imageMetadataRequest) throws IOException,
      ImageMgmtException {
    // Convert input json payload to image version update request object
    ImageVersionRequest imageVersionRequest =
        converterUtils.convertToModel(imageMetadataRequest.getJsonPayload(),
            ImageVersionRequest.class);
    // Set the user who invoked the update API
    imageVersionRequest.setModifiedBy(imageMetadataRequest.getUser());
    imageVersionRequest.setId((Integer) imageMetadataRequest.getParams().get(ID_KEY));
    log.info("imageVersionUpdateRequest: " + imageVersionRequest);
    // Input validation for image version create request
    final List<String> validationErrors = new ArrayList<>();
    if (!ValidatorUtils.validateObject(imageVersionRequest, validationErrors)) {
      String errors = validationErrors.stream().collect(Collectors.joining(","));
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST, String.format("Provide valid "
          + "input for creating image version metadata. Error(s): [%s].", errors));
    }
    imageVersionsDao.updateImageVersion(imageVersionRequest);
  }
}
