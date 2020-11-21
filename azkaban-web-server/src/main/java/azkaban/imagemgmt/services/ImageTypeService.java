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

import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.dto.ImageMetadataRequest;
import java.io.IOException;

/**
 * This service layer interface exposes methods for delegation and processing business logic for
 * image type APIs. For example API request processing and validation are handled in this layer.
 * Eventually the requests are routed to the DAO layer for data access.
 */
public interface ImageTypeService {

  /**
   * Method for processing and delegation of image type creation/registration
   * @param imageMetadataRequest - DTO which encaptulates  and transfers the create request details
   * @return int - id of the image type metadata record created
   * @throws IOException
   * @throws ImageMgmtException
   */
  public int createImageType(ImageMetadataRequest imageMetadataRequest) throws IOException,
      ImageMgmtException;
}
