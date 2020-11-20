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

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.exeception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageType;
import azkaban.imagemgmt.dto.RequestContext;
import azkaban.imagemgmt.utils.ConverterUtils;
import azkaban.imagemgmt.utils.ValidatorUtils;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This service layer implementation for processing and delegation of image type APIs. For
 * example API request processing and validation are handled in this layer. Eventually the
 * requests are routed to the DAO layer for data access.
 */
@Singleton
public class ImageTypeServiceImpl implements ImageTypeService {

  private final ImageTypeDao imageTypeDao;
  private final ConverterUtils converterUtils;

  @Inject
  public ImageTypeServiceImpl(final ImageTypeDao imageTypeDao, final ConverterUtils converterUtils) {
    this.imageTypeDao = imageTypeDao;
    this.converterUtils = converterUtils;
  }

  @Override
  public int createImageType(RequestContext requestContext) throws IOException,
      ImageMgmtException {
    // convert input json payload to image type object
    ImageType imageType = converterUtils.convertToModel(requestContext.getJsonPayload(), ImageType.class);
    imageType.setCreatedBy(requestContext.getUser());
    imageType.setModifiedBy(requestContext.getUser());
    // input validation for image type create request
    if(!ValidatorUtils.validateObject(imageType)) {
      throw new ImageMgmtValidationException("Provide valid input for creating image type "
          + "metadata");
    }
    return imageTypeDao.createImageType(imageType);
  }
}
