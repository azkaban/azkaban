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

import java.util.List;
import javax.validation.constraints.NotBlank;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This represents image rampup plan request and used while creating or updating image rampup plan
 * for an image type.
 */
public class ImageRampupPlanRequestDTO extends BaseDTO {

  // Represents the name of the ramp up plan
  @JsonProperty("planName")
  @NotBlank(message = "PlanName cannot be blank.", groups = ValidationOnCreate.class)
  private String planName;
  // Represents the name of the image type
  @JsonProperty("imageType")
  @NotBlank(message = "ImageType cannot be blank.", groups = {ValidationOnCreate.class,
      ValidationOnUpdate.class})
  private String imageTypeName;
  // Rampup plan description
  private String description;
  // This flag indicates if the plan needs to be activated or not
  @JsonProperty("activatePlan")
  private Boolean activatePlan;

  // This flag if set will forcefully activate the new plan by deactivating the existing active
  // rampup plan
  @JsonProperty("forceActivatePlan")
  private Boolean forceActivatePlan;

  // Rampup details for the image type
  private List<ImageRampupDTO> imageRampups;

  public String getPlanName() {
    return this.planName;
  }

  public void setPlanName(final String planName) {
    this.planName = planName;
  }

  public String getImageTypeName() {
    return this.imageTypeName;
  }

  public void setImageTypeName(final String imageTypeName) {
    this.imageTypeName = imageTypeName;
  }

  public String getDescription() {
    return this.description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public Boolean getActivatePlan() {
    return this.activatePlan;
  }

  public void setActivatePlan(final Boolean activatePlan) {
    this.activatePlan = activatePlan;
  }

  public Boolean getForceActivatePlan() {
    return this.forceActivatePlan;
  }

  public void setForceActivatePlan(final Boolean forceActivatePlan) {
    this.forceActivatePlan = forceActivatePlan;
  }

  public List<ImageRampupDTO> getImageRampups() {
    return this.imageRampups;
  }

  public void setImageRampups(final List<ImageRampupDTO> imageRampups) {
    this.imageRampups = imageRampups;
  }
}
