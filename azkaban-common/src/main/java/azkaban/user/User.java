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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class User {

  private final String userid;
  private final Set<String> roles = new HashSet<>();
  private final Set<String> groups = new HashSet<>();
  private final HashMap<String, String> properties = new HashMap<>();
  private String email = "";
  private UserPermissions userPermissions;

  public User(final String userid) {
    this.userid = userid;
  }

  public String getUserId() {
    return this.userid;
  }

  public String getEmail() {
    return this.email;
  }

  public void setEmail(final String email) {
    this.email = email;
  }

  public UserPermissions getPermissions() {
    return this.userPermissions;
  }

  public void setPermissions(final UserPermissions checker) {
    this.userPermissions = checker;
  }

  public boolean hasPermission(final String permission) {
    if (this.userPermissions == null) {
      return false;
    }
    return this.userPermissions.hasPermission(permission);
  }

  public List<String> getGroups() {
    return new ArrayList<>(this.groups);
  }

  public void clearGroup() {
    this.groups.clear();
  }

  public void addGroup(final String name) {
    this.groups.add(name);
  }

  public boolean isInGroup(final String group) {
    return this.groups.contains(group);
  }

  public List<String> getRoles() {
    return new ArrayList<>(this.roles);
  }

  public void addRole(final String role) {
    this.roles.add(role);
  }

  public boolean hasRole(final String role) {
    return this.roles.contains(role);
  }

  public String getProperty(final String name) {
    return this.properties.get(name);
  }

  @Override
  public String toString() {
    String groupStr = "[";
    for (final String group : this.groups) {
      groupStr += group + ",";
    }
    groupStr += "]";
    return this.userid + ": " + groupStr;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.userid == null) ? 0 : this.userid.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final User other = (User) obj;
    if (this.userid == null) {
      if (other.userid != null) {
        return false;
      }
    } else if (!this.userid.equals(other.userid)) {
      return false;
    }
    return true;
  }

  public static interface UserPermissions {

    public boolean hasPermission(String permission);

    public void addPermission(String permission);
  }

  public static class DefaultUserPermission implements UserPermissions {

    Set<String> permissions;

    public DefaultUserPermission() {
      this(new HashSet<>());
    }

    public DefaultUserPermission(final Set<String> permissions) {
      this.permissions = permissions;
    }

    @Override
    public boolean hasPermission(final String permission) {
      return this.permissions.contains(permission);
    }

    @Override
    public void addPermission(final String permission) {
      this.permissions.add(permission);
    }
  }
}
