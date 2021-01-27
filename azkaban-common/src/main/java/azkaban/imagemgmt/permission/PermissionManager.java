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


import azkaban.imagemgmt.exception.ImageMgmtException;
import azkaban.user.Permission.Type;

/**
 * Interface defines method to check the permission for accessing image management APIs.
 */
public interface PermissionManager {

  /**
   * Checks the permission based on image type name, user id and Permission type for accessing image
   * management APIs.
   *
   * @param imageTypeName
   * @param userId
   * @param type
   * @return boolean
   */
  public boolean hasPermission(final String imageTypeName, final String userId, final Type type)
      throws ImageMgmtException;

}
