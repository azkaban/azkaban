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

import azkaban.imagemgmt.models.ImageOwnership.Role;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class represents ownership information of an image type.
 */
public class ImageOwnershipDTO extends BaseDTO {

  // Represents image type name
  @JsonProperty("imageType")
  @NotBlank(message = "imageType cannot be blank.")
  private String name;
  // Image type owner
  @NotBlank(message = "Owner cannot be blank.")
  private String owner;
  // Role of the owner
  @NotNull(message = "Role cannot be null.")
  private Role role;

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getOwner() {
    return this.owner;
  }

  public void setOwner(final String owner) {
    this.owner = owner;
  }

  public Role getRole() {
    return this.role;
  }

  public void setRole(final Role role) {
    this.role = role;
  }

}
