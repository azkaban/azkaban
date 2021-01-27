/*
 * Copyright 2021 LinkedIn Corp.
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

import azkaban.imagemgmt.dto.ImageVersionMetadataResponseDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import java.util.Map;

public interface ImageVersionMetadataService {

  /**
   * Method for getting image version metadata such as version specific details, rampup information.
   * This method provides image version information and rampup details in
   * ImageVersionMetadataResponseDTO format and used as a response for dispaying on /status API
   * page.
   * @return Map<String, ImageVersionMetadataResponseDTO>
   * @throws ImageMgmtException
   */
  public Map<String, ImageVersionMetadataResponseDTO> getVersionMetadataForAllImageTypes()
      throws ImageMgmtException;
}
