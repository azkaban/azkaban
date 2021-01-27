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

import azkaban.imagemgmt.dto.ImageOwnershipDTO;
import azkaban.imagemgmt.dto.ImageTypeDTO;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageType;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;

@Singleton
public class ImageTypeConverter implements
    Converter<ImageTypeDTO, ImageTypeDTO, ImageType> {

  @Override
  public ImageType convertToDataModel(final ImageTypeDTO imageTypeRequest) {
    final ImageType imageType = new ImageType();
    imageType.setId(imageTypeRequest.getId());
    imageType.setCreatedBy(imageTypeRequest.getCreatedBy());
    imageType.setCreatedOn(imageTypeRequest.getCreatedOn());
    imageType.setModifiedBy(imageTypeRequest.getModifiedBy());
    imageType.setModifiedOn(imageTypeRequest.getModifiedOn());
    imageType.setName(imageTypeRequest.getName());
    imageType.setDescription(imageTypeRequest.getDescription());
    imageType.setDeployable(imageTypeRequest.getDeployable());
    imageType.setOwnerships(CollectionUtils.emptyIfNull(imageTypeRequest.getOwnerships()).stream()
        .map(imageOwnership -> convertToImageOwnershipModel(imageOwnership))
        .collect(Collectors.toList()));
    return imageType;
  }

  @Override
  public List<ImageType> convertToDataModels(final List<ImageTypeDTO> imageTypeDTOs) {
    return CollectionUtils.emptyIfNull(imageTypeDTOs).stream()
        .map(imageTypeDTO -> convertToDataModel(imageTypeDTO))
        .collect(Collectors.toList());
  }

  @Override
  public ImageTypeDTO convertToApiResponseDTO(final ImageType imageType) {
    final ImageTypeDTO imageTypeResponse = new ImageTypeDTO();
    imageTypeResponse.setId(imageType.getId());
    imageTypeResponse.setCreatedBy(imageType.getCreatedBy());
    imageTypeResponse.setCreatedOn(imageType.getCreatedOn());
    imageTypeResponse.setModifiedBy(imageType.getModifiedBy());
    imageTypeResponse.setModifiedOn(imageType.getModifiedOn());
    imageTypeResponse.setName(imageType.getName());
    imageTypeResponse.setDescription(imageType.getDescription());
    imageTypeResponse.setDeployable(imageType.getDeployable());
    imageTypeResponse.setOwnerships(CollectionUtils.emptyIfNull(imageType.getOwnerships()).stream()
        .map(imageOwnership -> convertToImageOwnershipResponse(imageOwnership))
        .collect(Collectors.toList()));
    return imageTypeResponse;
  }

  @Override
  public List<ImageTypeDTO> convertToApiResponseDTOs(final List<ImageType> imageTypes) {
    return CollectionUtils.emptyIfNull(imageTypes).stream()
        .map(imageType -> convertToApiResponseDTO(imageType))
        .collect(Collectors.toList());
  }

  /**
   * Converts from request ImageOwnershipDTO to ImageOwnership model.
   *
   * @param imageOwnershipRequest
   * @return ImageOwnership
   */
  private ImageOwnership convertToImageOwnershipModel(
      final ImageOwnershipDTO imageOwnershipRequest) {
    final ImageOwnership imageOwnership = new ImageOwnership();
    imageOwnership.setId(imageOwnershipRequest.getId());
    imageOwnership.setCreatedBy(imageOwnershipRequest.getCreatedBy());
    imageOwnership.setCreatedOn(imageOwnershipRequest.getCreatedOn());
    imageOwnership.setModifiedBy(imageOwnershipRequest.getModifiedBy());
    imageOwnership.setModifiedOn(imageOwnershipRequest.getModifiedOn());
    imageOwnership.setName(imageOwnershipRequest.getName());
    imageOwnership.setRole(imageOwnershipRequest.getRole());
    imageOwnership.setOwner(imageOwnershipRequest.getOwner());
    return imageOwnership;
  }

  /**
   * Converts from ImageOwnership model to ImageOwnershipDTO response.
   *
   * @param imageOwnership
   * @return ImageOwnershipDTO
   */
  private ImageOwnershipDTO convertToImageOwnershipResponse(
      final ImageOwnership imageOwnership) {
    final ImageOwnershipDTO imageOwnershipResponse = new ImageOwnershipDTO();
    imageOwnershipResponse.setId(imageOwnership.getId());
    imageOwnershipResponse.setCreatedBy(imageOwnership.getCreatedBy());
    imageOwnershipResponse.setCreatedOn(imageOwnership.getCreatedOn());
    imageOwnershipResponse.setModifiedBy(imageOwnership.getModifiedBy());
    imageOwnershipResponse.setModifiedOn(imageOwnership.getModifiedOn());
    imageOwnershipResponse.setName(imageOwnership.getName());
    imageOwnershipResponse.setRole(imageOwnership.getRole());
    imageOwnershipResponse.setOwner(imageOwnership.getOwner());
    return imageOwnershipResponse;
  }

}
