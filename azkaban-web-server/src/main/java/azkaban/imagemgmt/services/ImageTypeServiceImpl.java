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

import azkaban.imagemgmt.cache.ImageTypeCache;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * This service layer implementation for processing and delegation of image type APIs. For example
 * API request processing and validation are handled in this layer. Eventually the requests are
 * routed to the DAO layer for data access.
 */
@Singleton
public class ImageTypeServiceImpl extends AbstractBaseService<ImageType, Integer, String> implements
    ImageTypeService {

  private final ImageTypeDao imageTypeDao;
  private final ConverterUtils converterUtils;

  @Inject
  public ImageTypeServiceImpl(ImageTypeDao imageTypeDao,
      ConverterUtils converterUtils,
      ImageTypeCache imageTypeCache) {
    super(imageTypeDao, imageTypeCache);
    this.imageTypeDao = imageTypeDao;
    this.converterUtils = converterUtils;
  }

  @Override
  public int createImageType(ImageMetadataRequest imageMetadataRequest) throws IOException,
      ImageMgmtException {
    // Convert input json payload to image type object
    ImageType imageType = converterUtils
        .convertToModel(imageMetadataRequest.getJsonPayload(), ImageType.class);
    imageType.setCreatedBy(imageMetadataRequest.getUser());
    imageType.setModifiedBy(imageMetadataRequest.getUser());
    // Input validation for image type create request
    if (!ValidatorUtils.validateObject(imageType)) {
      throw new ImageMgmtValidationException("Provide valid input for creating image type "
          + "metadata");
    }
    Integer id = imageTypeDao.createImageType(imageType);
    // Trigger update to the cache
    notifyUpdate(id, imageType.getName(), NotifyAction.ADD);
    return id;
  }
}
