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
package azkaban.imagemgmt.dto;

import azkaban.imagemgmt.models.ImageRampup.StabilityTag;
import azkaban.imagemgmt.models.ImageVersion.State;
import java.util.List;

/**
 * This DTO class represents API specific image version metadata response.
 */
public class ImageVersionMetadataResponseDTO {

  // Represents version for an image type selected based on random rampup or current active version.
  private final String version;
  // Current state of the version.
  private final State state;
  private final List<RampupMetadata> rampups;

  public ImageVersionMetadataResponseDTO(final String version, final State state,
      final List<RampupMetadata> rampups) {
    this.version = version;
    this.state = state;
    this.rampups = rampups;
  }

  /**
   * Represents rampup metadata for an image type.
   */
  public static class RampupMetadata {

    private final String version;
    private final Integer rampupPercentage;
    private final StabilityTag stabilityTag;

    public RampupMetadata(final String version, final Integer rampupPercentage,
        final StabilityTag stabilityTag) {
      this.version = version;
      this.rampupPercentage = rampupPercentage;
      this.stabilityTag = stabilityTag;
    }

    public String getVersion() {
      return this.version;
    }

    public Integer getRampupPercentage() {
      return this.rampupPercentage;
    }

    public StabilityTag getStabilityTag() {
      return this.stabilityTag;
    }
  }
}
