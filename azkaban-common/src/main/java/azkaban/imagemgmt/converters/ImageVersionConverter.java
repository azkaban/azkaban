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

import azkaban.imagemgmt.dto.ImageVersionDTO;
import azkaban.imagemgmt.models.ImageVersion;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;

@Singleton
public class ImageVersionConverter implements Converter<ImageVersionDTO, ImageVersionDTO,
    ImageVersion> {

  @Override
  public ImageVersion convertToDataModel(final ImageVersionDTO imageVersionRequest) {
    final ImageVersion imageVersion = new ImageVersion();
    imageVersion.setId(imageVersionRequest.getId());
    imageVersion.setCreatedBy(imageVersionRequest.getCreatedBy());
    imageVersion.setCreatedOn(imageVersionRequest.getCreatedOn());
    imageVersion.setModifiedBy(imageVersionRequest.getModifiedBy());
    imageVersion.setModifiedOn(imageVersionRequest.getModifiedOn());
    imageVersion.setName(imageVersionRequest.getName());
    imageVersion.setDescription(imageVersionRequest.getDescription());
    imageVersion.setPath(imageVersionRequest.getPath());
    imageVersion.setVersion(imageVersionRequest.getVersion());
    imageVersion.setState(imageVersionRequest.getState());
    imageVersion.setReleaseTag(imageVersionRequest.getReleaseTag());
    return imageVersion;
  }

  @Override
  public List<ImageVersion> convertToDataModels(final List<ImageVersionDTO> imageVersionDTOs) {
    return CollectionUtils.emptyIfNull(imageVersionDTOs).stream()
        .map(imageVersionDTO -> convertToDataModel(imageVersionDTO))
        .collect(Collectors.toList());
  }

  @Override
  public ImageVersionDTO convertToApiResponseDTO(final ImageVersion imageVersion) {
    final ImageVersionDTO imageVersionResponse = new ImageVersionDTO();
    imageVersionResponse.setId(imageVersion.getId());
    imageVersionResponse.setCreatedBy(imageVersion.getCreatedBy());
    imageVersionResponse.setCreatedOn(imageVersion.getCreatedOn());
    imageVersionResponse.setModifiedBy(imageVersion.getModifiedBy());
    imageVersionResponse.setModifiedOn(imageVersion.getModifiedOn());
    imageVersionResponse.setName(imageVersion.getName());
    imageVersionResponse.setDescription(imageVersion.getDescription());
    imageVersionResponse.setPath(imageVersion.getPath());
    imageVersionResponse.setVersion(imageVersion.getVersion());
    imageVersionResponse.setState(imageVersion.getState());
    imageVersionResponse.setReleaseTag(imageVersion.getReleaseTag());
    return imageVersionResponse;
  }

  @Override
  public List<ImageVersionDTO> convertToApiResponseDTOs(final List<ImageVersion> imageVersions) {
    return CollectionUtils.emptyIfNull(imageVersions).stream()
        .map(imageVersion -> convertToApiResponseDTO(imageVersion))
        .collect(Collectors.toList());
  }
}
