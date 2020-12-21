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

import azkaban.imagemgmt.models.BaseModel;
import azkaban.imagemgmt.models.ImageRampup.StabilityTag;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class represents image rampup request used while creating or updating rampup details for an
 * image type.
 */
public class ImageRampupDTO extends BaseDTO {

  // Represents rampup plan id
  private int planId;
  // Image version
  @JsonProperty("imageVersion")
  @NotBlank(message = "ImageVersion cannot be blank.")
  private String imageVersion;
  @NotNull(message = "Rampup percentage cannot be null and must be between 0 to 100 (both "
      + "inclusive.)")
  private Integer rampupPercentage;
  // Stability tag of the version being ramped up
  private StabilityTag stabilityTag;

  public int getPlanId() {
    return this.planId;
  }

  public void setPlanId(final int planId) {
    this.planId = planId;
  }

  public String getImageVersion() {
    return this.imageVersion;
  }

  public void setImageVersion(final String imageVersion) {
    this.imageVersion = imageVersion;
  }

  public Integer getRampupPercentage() {
    return this.rampupPercentage;
  }

  public void setRampupPercentage(final Integer rampupPercentage) {
    this.rampupPercentage = rampupPercentage;
  }

  public StabilityTag getStabilityTag() {
    return this.stabilityTag;
  }

  public void setStabilityTag(final StabilityTag stabilityTag) {
    this.stabilityTag = stabilityTag;
  }

  @Override
  public String toString() {
    return "ImageRampup{" +
        "planId=" + this.planId +
        ", imageVersion='" + this.imageVersion + '\'' +
        ", stabilityTag=" + this.stabilityTag +
        ", id=" + this.id +
        ", createdBy='" + this.createdBy + '\'' +
        ", createdOn='" + this.createdOn + '\'' +
        ", modifiedBy='" + this.modifiedBy + '\'' +
        ", modifiedOn='" + this.modifiedOn + '\'' +
        '}';
  }
}
