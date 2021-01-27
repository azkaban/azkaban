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
package azkaban.imagemgmt.converters;

import azkaban.imagemgmt.dto.ImageRampupDTO;
import azkaban.imagemgmt.dto.ImageRampupPlanRequestDTO;
import azkaban.imagemgmt.dto.ImageRampupPlanResponseDTO;
import azkaban.imagemgmt.models.ImageRampup;
import azkaban.imagemgmt.models.ImageRampupPlan;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;

@Singleton
public class ImageRampupPlanConverter implements Converter<ImageRampupPlanRequestDTO,
    ImageRampupPlanResponseDTO, ImageRampupPlan> {

  @Override
  public ImageRampupPlan convertToDataModel(
      final ImageRampupPlanRequestDTO imageRampupPlanRequest) {
    final ImageRampupPlan imageRampupPlan = new ImageRampupPlan();
    imageRampupPlan.setId(imageRampupPlanRequest.getId());
    imageRampupPlan.setCreatedBy(imageRampupPlanRequest.getCreatedBy());
    imageRampupPlan.setCreatedOn(imageRampupPlanRequest.getCreatedOn());
    imageRampupPlan.setModifiedBy(imageRampupPlanRequest.getModifiedBy());
    imageRampupPlan.setModifiedOn(imageRampupPlanRequest.getModifiedOn());
    imageRampupPlan.setPlanName(imageRampupPlanRequest.getPlanName());
    imageRampupPlan.setDescription(imageRampupPlanRequest.getDescription());
    imageRampupPlan.setActivatePlan(imageRampupPlanRequest.getActivatePlan());
    imageRampupPlan.setForceActivatePlan(imageRampupPlanRequest.getForceActivatePlan());
    imageRampupPlan.setImageTypeName(imageRampupPlanRequest.getImageTypeName());
    imageRampupPlan.setImageRampups(
        CollectionUtils.emptyIfNull(imageRampupPlanRequest.getImageRampups()).stream()
            .map(imageRampup -> convertToRampupModel(imageRampup)).collect(Collectors.toList()));
    return imageRampupPlan;
  }

  @Override
  public List<ImageRampupPlan> convertToDataModels(
      final List<ImageRampupPlanRequestDTO> imageRampupPlanRequestDTOs) {
    return CollectionUtils.emptyIfNull(imageRampupPlanRequestDTOs).stream()
        .map(imageRampupPlanRequestDTO -> convertToDataModel(imageRampupPlanRequestDTO))
        .collect(Collectors.toList());
  }

  @Override
  public ImageRampupPlanResponseDTO convertToApiResponseDTO(final ImageRampupPlan imageRampupPlan) {
    final ImageRampupPlanResponseDTO imageRampupPlanResponse = new ImageRampupPlanResponseDTO();
    imageRampupPlanResponse.setId(imageRampupPlan.getId());
    imageRampupPlanResponse.setCreatedBy(imageRampupPlan.getCreatedBy());
    imageRampupPlanResponse.setCreatedOn(imageRampupPlan.getCreatedOn());
    imageRampupPlanResponse.setModifiedBy(imageRampupPlan.getModifiedBy());
    imageRampupPlanResponse.setModifiedOn(imageRampupPlan.getModifiedOn());
    imageRampupPlanResponse.setPlanName(imageRampupPlan.getPlanName());
    imageRampupPlanResponse.setDescription(imageRampupPlan.getDescription());
    imageRampupPlanResponse.setActive(imageRampupPlan.isActive());
    imageRampupPlanResponse.setImageTypeName(imageRampupPlan.getImageTypeName());
    imageRampupPlanResponse
        .setImageRampups(CollectionUtils.emptyIfNull(imageRampupPlan.getImageRampups()).stream()
            .map(imageRampup -> convertToRampupResponseDTO(imageRampup))
            .collect(Collectors.toList()));
    return imageRampupPlanResponse;
  }

  @Override
  public List<ImageRampupPlanResponseDTO> convertToApiResponseDTOs(
      final List<ImageRampupPlan> imageRampupPlans) {
    return null;
  }

  /**
   * Converts from image rampup request to image rampup model.
   *
   * @param imageRampupRequest
   * @return ImageRampup
   */
  private ImageRampup convertToRampupModel(final ImageRampupDTO imageRampupRequest) {
    final ImageRampup imageRampup = new ImageRampup();
    imageRampup.setId(imageRampupRequest.getId());
    imageRampup.setCreatedBy(imageRampupRequest.getCreatedBy());
    imageRampup.setCreatedOn(imageRampupRequest.getCreatedOn());
    imageRampup.setModifiedBy(imageRampupRequest.getModifiedBy());
    imageRampup.setModifiedOn(imageRampupRequest.getModifiedOn());
    imageRampup.setPlanId(imageRampupRequest.getPlanId());
    imageRampup.setImageVersion(imageRampupRequest.getImageVersion());
    imageRampup.setRampupPercentage(imageRampupRequest.getRampupPercentage());
    imageRampup.setStabilityTag(imageRampupRequest.getStabilityTag());
    return imageRampup;
  }

  /**
   * Converts from image rampup model to image rampup response DTO.
   *
   * @param imageRampup
   * @return ImageRampupDTO
   */
  private ImageRampupDTO convertToRampupResponseDTO(final ImageRampup imageRampup) {
    final ImageRampupDTO imageRampupResponse = new ImageRampupDTO();
    imageRampupResponse.setId(imageRampup.getId());
    imageRampupResponse.setCreatedBy(imageRampup.getCreatedBy());
    imageRampupResponse.setCreatedOn(imageRampup.getCreatedOn());
    imageRampupResponse.setModifiedBy(imageRampup.getModifiedBy());
    imageRampupResponse.setModifiedOn(imageRampup.getModifiedOn());
    imageRampupResponse.setPlanId(imageRampup.getPlanId());
    imageRampupResponse.setImageVersion(imageRampup.getImageVersion());
    imageRampupResponse.setRampupPercentage(imageRampup.getRampupPercentage());
    imageRampupResponse.setStabilityTag(imageRampup.getStabilityTag());
    return imageRampupResponse;
  }
}
