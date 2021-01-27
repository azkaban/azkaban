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

/**
 * This class represents image rampup plan metadata.
 */
public class ImageRampupPlan extends BaseModel {

  // Represents the name of the ramp up plan
  private String planName;
  // Represents the name of the image type
  private String imageTypeName;
  // Rampup plan description
  private String description;
  // Indicates if the plan is active or not.
  private boolean active;
  // User specified flag to activate the plan
  private Boolean activatePlan;
  // This flag if set will forcefully activate the new plan by deactivating the existing active
  // rampup plan
  private Boolean forceActivatePlan;
  // Rampups for the image type
  private List<ImageRampup> imageRampups;

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

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public Boolean getActivatePlan() {
    return activatePlan;
  }

  public void setActivatePlan(Boolean activatePlan) {
    this.activatePlan = activatePlan;
  }

  public Boolean getForceActivatePlan() {
    return forceActivatePlan;
  }

  public void setForceActivatePlan(Boolean forceActivatePlan) {
    this.forceActivatePlan = forceActivatePlan;
  }

  public List<ImageRampup> getImageRampups() {
    return imageRampups;
  }

  public void setImageRampups(List<ImageRampup> imageRampups) {
    this.imageRampups = imageRampups;
  }

  @Override
  public String toString() {
    return "ImageRampupPlan{" +
        "planName='" + planName + '\'' +
        ", imageTypeName='" + imageTypeName + '\'' +
        ", description='" + description + '\'' +
        ", active=" + active +
        ", imageRampups=" + imageRampups +
        ", id=" + id +
        ", createdBy='" + createdBy + '\'' +
        ", createdOn='" + createdOn + '\'' +
        ", modifiedBy='" + modifiedBy + '\'' +
        ", modifiedOn='" + modifiedOn + '\'' +
        '}';
  }
}
