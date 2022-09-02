/*
 * Copyright 2022 LinkedIn Corp.
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

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class represents ImageRampRule Request when receiving request body from user.
 * */
public class ImageRampRuleRequestDTO extends BaseDTO {
  // Represents the name of the Ramp rule
  @JsonProperty("ruleName")
  @NotBlank(message = "ruleName cannot be blank.", groups = ValidationOnCreate.class)
  @NotNull(message = "ruleName cannot be null.")
  private String ruleName;
  // Represents the name of the image type for the rule
  @JsonProperty("imageName")
  @NotBlank(message = "imageName cannot be blank.", groups = {ValidationOnCreate.class})
  @NotNull(message = "imageName cannot be null.")
  private String imageName;
  // Represents the name of the image version for the rule
  @JsonProperty("imageVersion")
  @NotBlank(message = "imageVersion cannot be blank.", groups = {ValidationOnCreate.class})
  @NotNull(message = "imageVersion cannot be null.")
  private String imageVersion;

  public void setRuleName(String ruleName) {
    this.ruleName = ruleName;
  }

  public void setImageName(String imageName) {
    this.imageName = imageName;
  }

  public void setImageVersion(String imageVersion) {
    this.imageVersion = imageVersion;
  }

  public String getRuleName() {
    return ruleName;
  }

  public String getImageName() {
    return imageName;
  }

  public String getImageVersion() {
    return imageVersion;
  }

}
