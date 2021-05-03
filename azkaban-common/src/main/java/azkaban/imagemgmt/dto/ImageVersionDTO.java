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

import azkaban.imagemgmt.models.ImageVersion.State;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This is image version request class for updating image version metadata.
 */
public class ImageVersionDTO extends BaseDTO {

  @JsonProperty("imageType")
  @NotBlank(message = "imageType cannot be blank.", groups = {ValidationOnCreate.class,
      ValidationOnUpdate.class})
  private String name;
  @JsonProperty("imagePath")
  @Size(max = 1024, message = "Path must not exceed 1024 characters.")
  private String path;
  // Represents image version. Version is in major.minor.patch.hotfix format
  @JsonProperty("imageVersion")
  @NotBlank(message = "imageVersion cannot be blank.", groups = ValidationOnCreate.class)
  @Pattern(regexp = "^(\\d+\\.)?(\\d+\\.)?(\\d+\\.)?(\\d+)$", message = "ImageVersion must be in "
      + "major.minor.patch.hotfix format (ex. 0.1, 1.2, 1.2.5, 1.2.3.4 etc.).",
      groups = ValidationOnCreate.class)
  private String version;
  // Description of the image version
  @Size(max = 512, message = "Description must not exceed 512 characters.")
  private String description;
  // State of the image version
  @JsonProperty("versionState")
  @NotNull(message = "VersionState cannot be blank.", groups = ValidationOnUpdate.class)
  private State state;
  /**
   * Associated release of the image version. For example, in case of azkaban images the
   * corresponding azkaban release number. In case of job type release, the corresponding release
   * number
   */
  private String releaseTag;

  public String getPath() {
    return this.path;
  }

  public void setPath(final String path) {
    this.path = path;
  }

  public String getVersion() {
    return this.version;
  }

  public void setVersion(final String version) {
    this.version = version;
  }

  public String getDescription() {
    return this.description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public State getState() {
    return this.state;
  }

  public void setState(final State state) {
    this.state = state;
  }

  public String getReleaseTag() {
    return this.releaseTag;
  }

  public void setReleaseTag(final String releaseTag) {
    this.releaseTag = releaseTag;
  }

  @Override
  public String toString() {
    return "ImageVersionDTO{" +
        "path='" + this.path + '\'' +
        ", description='" + this.description + '\'' +
        ", state=" + this.state +
        ", id=" + this.id +
        ", createdBy='" + this.createdBy + '\'' +
        ", createdOn='" + this.createdOn + '\'' +
        ", modifiedBy='" + this.modifiedBy + '\'' +
        ", modifiedOn='" + this.modifiedOn + '\'' +
        '}';
  }
}
