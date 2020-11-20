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

import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.dto.RequestContext;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.io.IOException;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service layer implementation for processing and delegation of image version APIs. For
 * example API request processing and validation are handled in this layer. Eventually the
 * requests are routed to the DAO layer for data access.
 */
@Singleton
public class ImageVersionServiceImpl implements ImageVersionService {

  private static final Logger log = LoggerFactory.getLogger(ImageVersionServiceImpl.class);

  private final ImageVersionDao imageVersionsDao;
  private final ConverterUtils converterUtils;

  @Inject
  public ImageVersionServiceImpl(final ImageVersionDao imageVersionsDao, final ConverterUtils converterUtils) {
    this.imageVersionsDao = imageVersionsDao;
    this.converterUtils = converterUtils;
  }

  @Override
  public int createImageVersion(RequestContext requestContext)
      throws IOException, ImageMgmtException {
    // Convert input json payload to image version object
    ImageVersion imageVersion = converterUtils.convertToModel(requestContext.getJsonPayload(),
        ImageVersion.class);
    // set the user who invoked the API
    imageVersion.setCreatedBy(requestContext.getUser());
    imageVersion.setModifiedBy(requestContext.getUser());
    // Override the state to NEW during creation of new image version
    imageVersion.setState(State.NEW);
    // input validation for image version create request
    if(!ValidatorUtils.validateObject(imageVersion)) {
      throw new ImageMgmtValidationException("Provide valid input for creating image version "
          + "metadata");
    }
    return imageVersionsDao.createImageVersion(imageVersion);
  }

  @Override
  public List<ImageVersion> findImageVersions(RequestContext requestContext) throws ImageMgmtException {
    return imageVersionsDao.findImageVersions(requestContext);
  }
}
