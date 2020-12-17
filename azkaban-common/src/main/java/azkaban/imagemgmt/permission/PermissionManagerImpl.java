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
import azkaban.imagemgmt.exeception.ErrorCode;
import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.ImageOwnership;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import java.util.Optional;
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

  @Inject
  public PermissionManagerImpl(final ImageTypeDao imageTypeDao) {
    this.imageTypeDao = imageTypeDao;
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
    final Optional<ImageOwnership> optionalImageOwnership =
        this.imageTypeDao.getImageTypeOwnership(imageTypeName, userId);
    boolean hasPermission = false;
    // Check if ownership is present. If so check the permission of the user role.
    if (optionalImageOwnership.isPresent()) {
      // Gets the permission of the user based on image type ownership metadata.
      final Permission permission = optionalImageOwnership.get().getRole().getPermission();
      // Check if the given Permission.Type contains in the permission of the user.
      if (permission.isPermissionSet(type)) {
        hasPermission = true;
      }
    } else {
      log.error(String.format("API access permission check failed. There is no ownership record "
          + "for image type: %s, user id: %s.", imageTypeName, userId));
      throw new ImageMgmtException(ErrorCode.FORBIDDEN, String.format("API access permission "
              + "check failed. There is no ownership record for image type: %s, user id: %s.",
          imageTypeName, userId));
    }

    return hasPermission;
  }
}
