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

import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.imagemgmt.exception.ImageMgmtValidationException;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.user.UserManager;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
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

  private final ImageTypeDao imageTypeDao;
  private final UserManager userManager;

  @Inject
  public PermissionManagerImpl(final ImageTypeDao imageTypeDao, final UserManager userManager) {
    this.imageTypeDao = imageTypeDao;
    this.userManager = userManager;
  }

  /**
   * Checks the permission based on user manager, image type name, user id and Permission type.
   *
   * @param imageTypeName
   * @param userId
   * @param type
   * @return boolean
   */
  @Override
  public boolean hasPermissionForImageType(final String imageTypeName, final String userId, final Type type) {
    // Gets the image type metadata including ownerships.
    final List<ImageOwnership> imageOwnerships =
        this.imageTypeDao.getImageTypeOwnership(imageTypeName);
    // The owner set contains both users and groups.
    final Set<String> ownerSet = new HashSet<>();
    // Check if ownership is present. If so check the permission of the user role.
    for (final ImageOwnership imageOwnership: imageOwnerships) {
      // Gets the permission of the user based on image type ownership metadata.
      final Permission permission = imageOwnership.getRole().getPermission();
      // Check if the given Permission.Type contains in the permission of the user.
      if (permission.isPermissionSet(type)) {
        ownerSet.add(imageOwnership.getOwner());
      }
    }
    return ownerSet.contains(userId) || userManager.validateUserGroupMembership(userId, ownerSet);
  }

  /**
   * Checks the user permission based on image type name, user id and Permission type;
   * Azkaban admin would have permission and other user validates through image_ownership table,
   *
   *
   * @param imageTypeName
   * @param user
   * @return boolean
   */
  @Override
  public Set<String> validatePermissionAndGetOwnerships(final String imageTypeName, final User user)
      throws ImageMgmtException {
    String userId = user.getUserId();
    // fetch owners from image_ownerships with matching permission set (e.g. ADMIN)
    Set<String> imageOwnerships = imageTypeDao.getImageTypeOwnership(imageTypeName).stream()
        .map(ImageOwnership::getOwner)
        .collect(Collectors.toSet());
    // not authorized if not azkaban admin nor validate through image_ownership
    if (!hasPermission(user, imageOwnerships)) {
      String errorMsg = String.format("unauthorized user %s does not has permission to operate", userId);
      log.error(errorMsg);
      throw new ImageMgmtValidationException(ErrorCode.UNAUTHORIZED, errorMsg);
    }
    return imageOwnerships;
  }

  /**
   * Method to check if user is Azkaban Admin.
   *
   * @param user
   * @return true, if azkaban dev
   *         false otherwise
   */
  @Override
  public boolean isAzkabanAdmin(final User user) {
    return user.getRoles().stream()
        .anyMatch(role -> userManager.getRole(role).getPermission().isPermissionSet(Type.ADMIN));
  }

  /**
   * Validate each userId's identity using userManager. userId can be group or individual user account.
   *
   * @param ids
   * @throws ImageMgmtValidationException
   */
  @Override
  public void validateIdentity(List<String> ids) {
    Set<String> invalids = ids.stream()
        .filter(id -> !userManager.validateGroup(id) && !userManager.validateUser(id))
        .collect(Collectors.toSet());
    if (!invalids.isEmpty()) {
      log.error("Invalid identity: " + invalids);
      throw new ImageMgmtValidationException(ErrorCode.BAD_REQUEST,
          "fail to validate identity: " + String.join(",", invalids));
    }
  }

  @Override
  public boolean hasPermission(User user, Set<String> currentOwners) {
    String userId = user.getUserId();
    return isAzkabanAdmin(user) || currentOwners.contains(userId)
        || userManager.validateUserGroupMembership(userId, currentOwners);
  }
}
