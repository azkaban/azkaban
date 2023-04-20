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

import java.util.Set;

/**
 * Interface for the UserManager. Implementors will have to handle the retrieval of the User object
 * given the username and password.
 *
 * The constructor will be called with a azkaban.utils.Props object passed as the only parameter. If
 * such a constructor doesn't exist, than the UserManager instantiation may fail.
 */
public interface UserManager {

  /**
   * Retrieves the user given the username and password to authenticate against.
   *
   * @throws UserManagerException If the username/password combination doesn't exist.
   */
  public User getUser(String username, String password)
      throws UserManagerException;

  /**
   * Returns true if the user is valid. This is used when adding permissions for users
   */
  public boolean validateUser(String username);

  /**
   * Returns true if the group is valid. This is used when adding permissions for groups.
   */
  public boolean validateGroup(String group);
  /**
   * Check whether a given group is a valid ldap group account.
   *
   * @param group Ldap group account name
   * @return True, if given group is a valid ldap group.
   *         False, otherwise
   * */
  public boolean validateLdapGroup(String group);

  /**
   * Returns the user role. This may return null.
   */
  public Role getRole(String roleName);

  public boolean validateProxyUser(String proxyUser, User realUser);

  /**
   * @param username e.g. user alias
   * @param groupSet e.g. a set of hadoop headless group / LDAP group names
   * @return Returns true if the user belongs to a group. This is used when verifying user
   * permission by checking its group membership
   */
  public boolean validateUserGroupMembership(String username, Set<String> groupSet);
}
