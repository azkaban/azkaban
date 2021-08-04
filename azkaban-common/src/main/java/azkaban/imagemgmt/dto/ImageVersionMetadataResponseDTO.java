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
package azkaban.imagemgmt.dto;

import azkaban.imagemgmt.models.ImageRampup.StabilityTag;
import azkaban.imagemgmt.models.ImageVersion.State;
import java.util.List;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonPropertyOrder;

/**
 * This DTO class represents API specific image version metadata response.
 */
@JsonPropertyOrder({"version", "state", "path", "releaseTag", "message", "rampups"})
public class ImageVersionMetadataResponseDTO {

  // Represents version for an image type selected based on deterministic rampup or current active
  //version.
  @JsonProperty("latestVersion")
  private final String version;
  // Current state of the version.
  @JsonProperty("state")
  private final State state;
  // Represents image path
  @JsonProperty("path")
  private final String path;
  @JsonProperty("rampups")
  private final List<RampupMetadata> rampups;
  // Captures the information about version selection process such as the version is based on
  // either random rampup or based on latest available active version.
  @JsonProperty("message")
  private final String message;
  @JsonProperty("releaseTag")
  private final String releaseTag;

  public ImageVersionMetadataResponseDTO(final String version, final State state, final String path,
      final List<RampupMetadata> rampups, final String message, final String releaseTag) {
    this.version = version;
    this.state = state;
    this.path = path;
    this.rampups = rampups;
    this.message = message;
    this.releaseTag = releaseTag;
  }

  public String getReleaseTag() {
    return releaseTag;
  }

  public String getVersion() {
    return version;
  }

  public State getState() {
    return state;
  }

  public String getPath() {
    return path;
  }

  public List<RampupMetadata> getRampups() {
    return rampups;
  }

  public String getMessage() {
    return message;
  }

  /**
   * Represents rampup metadata for an image type.
   */
  @JsonPropertyOrder({"version", "stabilityTag", "releaseTag", "rampupPercentage"})
  public static class RampupMetadata {

    @JsonProperty("version")
    private final String version;
    @JsonProperty("rampupPercentage")
    private final Integer rampupPercentage;
    @JsonProperty("stabilityTag")
    private final StabilityTag stabilityTag;
    @JsonProperty("releaseTag")
    private final String releaseTag;

    public RampupMetadata(final String version, final Integer rampupPercentage,
        final StabilityTag stabilityTag, final String releaseTag) {
      this.version = version;
      this.rampupPercentage = rampupPercentage;
      this.stabilityTag = stabilityTag;
      this.releaseTag = releaseTag;
    }

    public String getVersion() {
      return this.version;
    }

    public String getReleaseTag() {
      return this.releaseTag;
    }

    public Integer getRampupPercentage() {
      return this.rampupPercentage;
    }

    public StabilityTag getStabilityTag() {
      return this.stabilityTag;
    }
  }
}
