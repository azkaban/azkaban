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

import javax.validation.constraints.NotBlank;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class represents ownership information of the image type
 */
public class ImageOwnership extends BaseModel {
  // Represents image type name
  @JsonProperty("imageType")
  @NotBlank(message = "imageType cannot be blank")
  private String name;
  // image type owner
  private String owner;
  // role of the owner
  private Role role;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public Role getRole() {
    return role;
  }

  public void setRole(Role role) {
    this.role = role;
  }

  /**
   * Enum representing owner Role
   * ADMIN role is having all the permissions such as invoking image management APIs, Adding new
   * job type owner etc.
   * MEMBER - Must have permissions to invoke image management APIs, but can't add/delete job
   * type owners etc. However the MEMBER role needs to be clearly defined.
   */
  public enum Role {
    ADMIN("admin"),
    MEMBER("member");

    private final String name;
    private Role(final String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  @Override
  public String toString() {
    return "ImageOwnership{" +
        "owner='" + owner + '\'' +
        ", role=" + role +
        ", id=" + id +
        ", createdBy='" + createdBy + '\'' +
        ", createdOn='" + createdOn + '\'' +
        ", modifiedBy='" + modifiedBy + '\'' +
        ", modifiedOn='" + modifiedOn + '\'' +
        '}';
  }
}
