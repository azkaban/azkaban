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
package azkaban.imagemgmt.models;

import azkaban.imagemgmt.models.ImageRampup.StabilityTag;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class represents image rampup request used while creating or updating rampup details for an
 * image type.
 */
public class ImageRampupRequest extends BaseModel {

  // Represents rampup plan id
  private int planId;
  // Image version
  @JsonProperty("imageVersion")
  @NotBlank(message = "ImageVersion cannot be blank")
  private String imageVersion;
  @NotNull(message = "Must specify rampup percentage")
  private Integer rampupPercentage;
  // Stability tag of the version being ramped up
  private StabilityTag stabilityTag;

  public int getPlanId() {
    return planId;
  }

  public void setPlanId(int planId) {
    this.planId = planId;
  }

  public String getImageVersion() {
    return imageVersion;
  }

  public void setImageVersion(String imageVersion) {
    this.imageVersion = imageVersion;
  }

  public Integer getRampupPercentage() {
    return rampupPercentage;
  }

  public void setRampupPercentage(Integer rampupPercentage) {
    this.rampupPercentage = rampupPercentage;
  }

  public StabilityTag getStabilityTag() {
    return stabilityTag;
  }

  public void setStabilityTag(StabilityTag stabilityTag) {
    this.stabilityTag = stabilityTag;
  }

  @Override
  public String toString() {
    return "ImageRampup{" +
        "planId=" + planId +
        ", imageVersion='" + imageVersion + '\'' +
        ", stabilityTag=" + stabilityTag +
        ", id=" + id +
        ", createdBy='" + createdBy + '\'' +
        ", createdOn='" + createdOn + '\'' +
        ", modifiedBy='" + modifiedBy + '\'' +
        ", modifiedOn='" + modifiedOn + '\'' +
        '}';
  }
}
