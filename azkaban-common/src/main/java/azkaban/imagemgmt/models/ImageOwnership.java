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

import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import javax.validation.constraints.NotBlank;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class represents ownership information of an image type.
 */
public class ImageOwnership extends BaseModel {

  // Represents image type name
  @JsonProperty("imageType")
  @NotBlank(message = "imageType cannot be blank")
  private String name;
  // Image type owner
  private String owner;
  // Role of the owner
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

  /**
   * Enum representing owner Role as ADMIN is having all the permissions such as invoking image
   * management APIs, including adding/removing image type owners etc. MEMBER role is having
   * permissions to invoke image management APIs, but can't add/delete image type owners etc.
   * GUEST role is having readonly or get access to the image management APIs.
   */
  public enum Role {
    ADMIN(ADMIN_PERMISSION),
    MEMBER(MEMBER_PERMISSION),
    GUEST(GUEST_PERMISSION);

    @SuppressWarnings("ImmutableEnumChecker")
    private final Permission permission;

    private Role(final Permission permission) {
      this.permission = permission;
    }

    public Permission getPermission() {
      return this.permission;
    }
  }

  /**
   * Permission for image type owner of role ADMIN. ADMIN owner has complete access to image
   * management APIs including add/delete of new member as image type owner.
   */
  private static final Permission ADMIN_PERMISSION = new Permission(Type.CREATE, Type.GET,
      Type.UPDATE, Type.DELETE, Type.IMAGE_TYPE_ADD_MEMBER, Type.IMAGE_TYPE_DELETE_MEMBER);
  /**
   * Permission for image type owner of role MEMBER. MEMBER owner has complete access to image
   * management APIs except add/delete of new member as image type owner.
   */
  private static final Permission MEMBER_PERMISSION = new Permission(Type.CREATE, Type.GET,
      Type.UPDATE, Type.DELETE);
  /**
   * The default permission is only GET access to the image management APIs.
   */
  private static final Permission GUEST_PERMISSION = new Permission(Type.GET);

  @Override
  public String toString() {
    return "ImageOwnership{" +
        "owner='" + this.owner + '\'' +
        ", role=" + this.role +
        ", id=" + this.id +
        ", createdBy='" + this.createdBy + '\'' +
        ", createdOn='" + this.createdOn + '\'' +
        ", modifiedBy='" + this.modifiedBy + '\'' +
        ", modifiedOn='" + this.modifiedOn + '\'' +
        '}';
  }
}
