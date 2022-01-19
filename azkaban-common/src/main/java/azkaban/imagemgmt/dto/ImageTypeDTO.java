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

import azkaban.imagemgmt.models.ImageType.Deployable;
import java.util.List;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import org.codehaus.jackson.annotate.JsonProperty;

public class ImageTypeDTO extends BaseDTO {

  @JsonProperty("imageType")
  @NotBlank(message = "ImageType cannot be blank.")
  @Size(max = 64, message = "Name must not exceed 64 characters.")
  private String name;
  // Type description
  @NotBlank(message = "Description cannot be blank.")
  @Size(max = 512, message = "Description must not exceed 512 characters.")
  private String description;
  // Represents the actual deployable such as image, jar tar etc.
  private Deployable deployable;
  // Associated ownership information for the image type
  private List<ImageOwnershipDTO> ownerships;

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getDescription() {
    return this.description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public Deployable getDeployable() {
    return this.deployable;
  }

  public void setDeployable(final Deployable deployable) {
    this.deployable = deployable;
  }

  public List<ImageOwnershipDTO> getOwnerships() {
    return this.ownerships;
  }

  public void setOwnerships(final List<ImageOwnershipDTO> ownerships) {
            ownerships.stream().forEach(o -> o.setName(getName()));
            this.ownerships = ownerships;
  }
}
