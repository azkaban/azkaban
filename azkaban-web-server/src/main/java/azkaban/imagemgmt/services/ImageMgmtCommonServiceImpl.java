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

import static azkaban.Constants.ImageMgmtConstants.IMAGE_RAMPUP_PLAN;
import static azkaban.Constants.ImageMgmtConstants.IMAGE_VERSION;

import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.daos.ImageMgmtCommonDao;
import azkaban.imagemgmt.dto.DeleteResponse;
import azkaban.imagemgmt.dto.ImageRampupPlanRequestDTO;
import azkaban.imagemgmt.dto.ImageRampupPlanResponseDTO;
import azkaban.imagemgmt.dto.ImageVersionDTO;
import azkaban.imagemgmt.dto.ImageVersionUsageDataDTO;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageRampupPlan;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersionUsageData;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageMgmtCommonServiceImpl implements ImageMgmtCommonService {

  private static final Logger log = LoggerFactory.getLogger(ImageVersionServiceImpl.class);

  private final ImageMgmtCommonDao imageMgmtCommonDao;
  private final Converter<ImageVersionDTO, ImageVersionDTO, ImageVersion> versionConverter;
  private final Converter<ImageRampupPlanRequestDTO, ImageRampupPlanResponseDTO, ImageRampupPlan> rampupPlanConverter;

  @Inject
  public ImageMgmtCommonServiceImpl(final ImageMgmtCommonDao imageMgmtCommonDao,
      @Named(IMAGE_VERSION) final Converter versionConverter,
      @Named(IMAGE_RAMPUP_PLAN) final Converter rampupPlanConverter) {
    this.imageMgmtCommonDao = imageMgmtCommonDao;
    this.versionConverter = versionConverter;
    this.rampupPlanConverter = rampupPlanConverter;
  }

  @Override
  public DeleteResponse deleteImageVersion(String imageType, int versionId, Boolean forceDelete)
      throws ImageMgmtException {
    DeleteResponse deleteResponse = this.imageMgmtCommonDao.deleteImageVersion(imageType,
        versionId, forceDelete);
    // Check if there are errors and data. If present convert to API specific response
    if(deleteResponse.hasErrors() && deleteResponse.getData().isPresent()) {
      ImageVersionUsageData imageVersionUsageData =
          (ImageVersionUsageData)deleteResponse.getData().get();
      ImageVersionDTO imageVersionDTO = imageVersionUsageData.getImageVersion() == null ? null :
      this.versionConverter.convertToApiResponseDTO(imageVersionUsageData.getImageVersion());
      List<ImageRampupPlanResponseDTO> imageRampupPlanResponseDTOs =
          imageVersionUsageData.getImageRampupPlans() == null ? null :
      this.rampupPlanConverter.convertToApiResponseDTOs(imageVersionUsageData.getImageRampupPlans());
      ImageVersionUsageDataDTO imageVersionUsageDataDTO = new ImageVersionUsageDataDTO(
          imageVersionDTO, imageRampupPlanResponseDTOs);
      deleteResponse.setData(imageVersionUsageDataDTO);
    }
    return deleteResponse;
  }
}
