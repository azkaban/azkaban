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
import azkaban.imagemgmt.dto.ImageVersionMetadataResponseDTO.RampupMetadata;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageVersion;
import azkaban.imagemgmt.models.ImageVersion.State;
import azkaban.imagemgmt.models.ImageVersionMetadata;
import azkaban.imagemgmt.rampup.ImageRampupManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Service layer implementation class for getting image version specific metadata such as version
 * specific details, rampup information etc.
 */
public class ImageVersionMetadataServiceImpl implements ImageVersionMetadataService {

  private final ImageRampupManager imageRampupManager;

  @Inject
  public ImageVersionMetadataServiceImpl(final ImageRampupManager imageRampupManager) {
    this.imageRampupManager = imageRampupManager;
  }

  /**
   * Method for getting image version metadata such as version specific details, rampup information.
   * This method provides image version information and rampup details in
   * ImageVersionMetadataResponseDTO format and used as a response for dispaying on /status API
   * page.
   *
   * @return
   * @throws ImageMgmtException
   */
  @Override
  public Map<String, ImageVersionMetadataResponseDTO> getVersionMetadataForAllImageTypes()
      throws ImageMgmtException {
    final Map<String, ImageVersionMetadata> versionMetadataMap =
        this.imageRampupManager.getVersionMetadataForAllImageTypes();
    final Map<String, ImageVersionMetadataResponseDTO> versionMetadataResponseMap = new TreeMap<>();
    versionMetadataMap.forEach((k, v) -> versionMetadataResponseMap.put(k,
        convertToVersionMetadataResponse(v)));
    return versionMetadataResponseMap;
  }

  private ImageVersionMetadataResponseDTO convertToVersionMetadataResponse(
      final ImageVersionMetadata versionMetadata) {
    List<RampupMetadata> rampups = new ArrayList<>();
    if (!CollectionUtils.isEmpty(versionMetadata.getImageRampups())) {
      rampups = versionMetadata.getImageRampups().stream().map(
          imageRampup -> new RampupMetadata(imageRampup.getImageVersion(),
              imageRampup.getRampupPercentage(), imageRampup.getStabilityTag()))
          .collect(Collectors.toList());
    }
    final ImageVersion imageVersion = versionMetadata.getImageVersion();
    final String version = imageVersion != null ? imageVersion.getVersion() : null;
    final State state = imageVersion != null ? imageVersion.getState() : null;
    final String path = imageVersion != null ? imageVersion.getPath() : null;
    return new ImageVersionMetadataResponseDTO(version, state, path, rampups,
        versionMetadata.getMessage());
  }
}
