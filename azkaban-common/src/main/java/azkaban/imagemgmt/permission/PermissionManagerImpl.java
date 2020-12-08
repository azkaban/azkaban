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
package azkaban.imagemgmt.permission;

import azkaban.imagemgmt.cache.ImageTypeCache;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.imagemgmt.models.ImageOwnership.Role;
import azkaban.imagemgmt.models.ImageType;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class defines and manages the permission for accessing image management APIs. The
 * permissions are defined on image type. The permissions are created using user role and access
 * type. This class internally accesses image type cache to get image type metadata.
 */
@Singleton
public class PermissionManagerImpl implements PermissionManager {

  private static final Logger log = LoggerFactory.getLogger(PermissionManagerImpl.class);

  private final ImageTypeCache imageTypeCache;

  /**
   * Permission for image type owner of role ADMIN. ADMIN owner has complete access to image
   * management APIs including add/delete of new member as image type owner.
   */
  private static final Permission ADMIN_PERMISSION = new Permission(Type.ADMIN);
  /**
   * Permission for image type owner of role MEMBER. MEMBER owner has complete access to image
   * management APIs except add/delete of new member as image type owner.
   */
  private static final Permission MEMBER_PERMISSION = new Permission(Type.CREATE, Type.GET,
      Type.UPDATE, Type.DELETE);
  /**
   * The default permission is only GET access to the image management APIs.
   */
  private static final Permission DEFAULT_PERMISSION = new Permission(Type.GET);

  @Inject
  public PermissionManagerImpl(final ImageTypeCache imageTypeCache) {
    this.imageTypeCache = imageTypeCache;
  }

  /**
   * Checks the permission based on image type name, user id and Permission type.
   *
   * @param imageTypeName
   * @param userId
   * @param type
   * @return boolean
   */
  @Override
  public boolean hasPermission(final String imageTypeName, final String userId, final Type type) {
    // Gets the image type metadata including ownerships.
    final Optional<ImageType> optionalImageType =
        imageTypeCache.getImageTypeWithOwnershipsByName(imageTypeName);
    boolean hasPermission = false;
    if (optionalImageType.isPresent()) {
      final ImageType imageType = optionalImageType.get();
      if (!CollectionUtils.isEmpty(imageType.getOwnerships())) {
        // Gets the permission of the user based on image type ownership metadata.
        final Optional<Permission> optionalPermission = getPermission(userId,
            imageType.getOwnerships());
        // Check if the given Permission.Type contains in the permission of the user.
        // Check if the the user permission contains Permission.Type ADMIN.
        if (optionalPermission.isPresent() && (optionalPermission.get().isPermissionSet(type)
            || optionalPermission.get().isPermissionSet(Permission.Type.ADMIN))) {
          hasPermission = true;
        }
      } else {
        log.info(String.format("API access permission check failed. There is no ownership record "
            + "for image type: %s.", imageTypeName));
        throw new ImageMgmtException(String.format("API access permission check failed. There is "
            + "no ownership record for image type: "
            + "%s.", imageTypeName));
      }
    } else {
      log.info(
          String.format("API access permission check failed. The image type metadata not found "
              + "for image type: %s.", imageTypeName));
      throw new ImageMgmtException(String.format("API access permission check failed. The image "
              + "type metadata not found for image type: %s.",
          imageTypeName));
    }
    return hasPermission;
  }

  /**
   * Gets the permission for the given user. Checks the permission of the given user based on image
   * ownership metadata.
   *
   * @param userId
   * @param imageOwnershipList
   * @return Optional<Permission>
   */
  private Optional<Permission> getPermission(final String userId,
      final List<ImageOwnership> imageOwnershipList) {
    for (final ImageOwnership imageOwnership : imageOwnershipList) {
      if (userId.equals(imageOwnership.getName())) {
        return getPermissionByRole(imageOwnership.getRole());
      }
    }
    return Optional.empty();
  }

  /**
   * Gets the permission based on user role.
   *
   * @param role
   * @return Optional<Permission>
   */
  private Optional<Permission> getPermissionByRole(final Role role) {
    Permission permission = null;
    switch (role) {
      case ADMIN:
        permission = ADMIN_PERMISSION;
        break;
      case MEMBER:
        permission = MEMBER_PERMISSION;
        break;
      default:
        permission = DEFAULT_PERMISSION;
    }
    return Optional.ofNullable(permission);
  }
}
