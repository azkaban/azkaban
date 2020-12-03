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

import java.util.List;
import javax.validation.constraints.NotBlank;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This represents image rampup plan request and used while creating or updating image rampup plan
 * for an image type.
 */
public class ImageRampupPlanRequest extends BaseModel {

  // Represents the name of the ramp up plan
  @JsonProperty("planName")
  @NotBlank(message = "PlanName cannot be blank")
  private String planName;
  // Represents the name of the image type
  @JsonProperty("imageType")
  @NotBlank(message = "ImageType cannot be blank")
  private String imageTypeName;
  // Rampup plan description
  private String description;
  // This flag indicates if the plan needs to be activated or not
  @JsonProperty("activatePlan")
  private boolean activatePlan;

  // Rampup details for the image type
  private List<ImageRampupRequest> imageRampups;

  public String getPlanName() {
    return planName;
  }

  public void setPlanName(String planName) {
    this.planName = planName;
  }

  public String getImageTypeName() {
    return imageTypeName;
  }

  public void setImageTypeName(String imageTypeName) {
    this.imageTypeName = imageTypeName;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public boolean isActivatePlan() {
    return activatePlan;
  }

  public void setActivatePlan(boolean activatePlan) {
    this.activatePlan = activatePlan;
  }

  public List<ImageRampupRequest> getImageRampups() {
    return imageRampups;
  }

  public void setImageRampups(List<ImageRampupRequest> imageRampups) {
    this.imageRampups = imageRampups;
  }
}
