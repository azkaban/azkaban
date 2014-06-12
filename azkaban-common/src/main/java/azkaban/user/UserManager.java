/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.user;

/**
 * Interface for the UserManager. Implementors will have to handle the retrieval
 * of the User object given the username and password.
 *
 * The constructor will be called with a azkaban.utils.Props object passed as
 * the only parameter. If such a constructor doesn't exist, than the UserManager
 * instantiation may fail.
 */
public interface UserManager {
  /**
   * Retrieves the user given the username and password to authenticate against.
   *
   * @param username
   * @param password
   * @return
   * @throws UserManagerException If the username/password combination doesn't
   *           exist.
   */
  public User getUser(String username, String password)
      throws UserManagerException;

  /**
   * Returns true if the user is valid. This is used when adding permissions for
   * users
   *
   * @param username
   * @return
   */
  public boolean validateUser(String username);

  /**
   * Returns true if the group is valid. This is used when adding permissions
   * for groups.
   *
   * @param group
   * @return
   */
  public boolean validateGroup(String group);

  /**
   * Returns the user role. This may return null.
   *
   * @param roleName
   * @return
   */
  public Role getRole(String roleName);

  public boolean validateProxyUser(String proxyUser, User realUser);
}
