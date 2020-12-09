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

import azkaban.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class Permission {

  private final Set<Type> permissions = new HashSet<>();

  public Permission() {
  }

  public Permission(final int flags) {
    setPermissions(flags);
  }

  public Permission(final Type... list) {
    addPermission(list);
  }

  public void addPermissions(final Permission perm) {
    permissions.addAll(perm.getTypes());
  }

  public void setPermission(final Type type, final boolean set) {
    if (set) {
      addPermission(type);
    } else {
      removePermissions(type);
    }
  }

  public void setPermissions(final int flags) {
    permissions.clear();
    if ((flags & Type.ADMIN.getFlag()) != 0) {
      addPermission(Type.ADMIN);
    } else {
      for (final Type type : Type.values()) {
        if ((flags & type.getFlag()) != 0) {
          addPermission(type);
        }
      }
    }
  }

  public void addPermission(final Type... list) {
    // Admin is all encompassing permission. No need to add other types
    if (!permissions.contains(Type.ADMIN)) {
      for (final Type perm : list) {
        permissions.add(perm);
      }
      // We add everything, and if there's Admin left, we make sure that only
      // Admin is remaining.
      if (permissions.contains(Type.ADMIN)) {
        permissions.clear();
        permissions.add(Type.ADMIN);
      }
    }
  }

  public void addPermissionsByName(final String... list) {
    for (final String perm : list) {
      final Type type = Type.valueOf(perm);
      if (type != null) {
        addPermission(type);
      }
    }
  }

  public void addPermissions(final Collection<Type> list) {
    for (final Type perm : list) {
      addPermission(perm);
    }
  }

  public void addPermissionsByName(final Collection<String> list) {
    for (final String perm : list) {
      final Type type = Type.valueOf(perm);
      if (type != null) {
        addPermission(type);
      }
    }
  }

  public Set<Type> getTypes() {
    return permissions;
  }

  public void removePermissions(final Type... list) {
    for (final Type perm : list) {
      permissions.remove(perm);
    }
  }

  public void removePermissionsByName(final String... list) {
    for (final String perm : list) {
      final Type type = Type.valueOf(perm);
      if (type != null) {
        permissions.remove(type);
      }
    }
  }

  public boolean isPermissionSet(final Type permission) {
    return permissions.contains(permission);
  }

  public boolean isPermissionNameSet(final String permission) {
    return permissions.contains(Type.valueOf(permission));
  }

  public String[] toStringArray() {
    final ArrayList<String> list = new ArrayList<>();
    int count = 0;
    for (final Type type : permissions) {
      list.add(type.toString());
      count++;
    }

    return list.toArray(new String[count]);
  }

  @Override
  public String toString() {
    return Utils.flattenToString(permissions, ",");
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result =
        prime * result + ((permissions == null) ? 0 : permissions.hashCode());
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
    final Permission other = (Permission) obj;
    if (permissions == null) {
      if (other.permissions != null) {
        return false;
      }
    } else if (!permissions.equals(other.permissions)) {
      return false;
    }
    return true;
  }

  public int toFlags() {
    int flag = 0;
    for (final Type type : permissions) {
      flag |= type.getFlag();
    }

    return flag;
  }

  public enum Type {
    READ(0x0000001),
    WRITE(0x0000002),
    EXECUTE(0x0000004),
    SCHEDULE(0x0000008),
    METRICS(0x0000010),
    CREATEPROJECTS(0x40000000), // Only used for roles
    // Users with this permission can upload projects when the property "lockdown.upload.projects"
    // is turned on
    UPLOADPROJECTS(0x0008000),
    ADMIN(0x8000000),
    // Permissions for image management APIs.
    CREATE(0x0000100),
    GET(0x0000200),
    UPDATE(0x0000300),
    DELETE(0x0000400),
    IMAGE_TYPE_ADD_MEMBER(0x0000500),
    IMAGE_TYPE_DELETE_MEMBER(0x0000600);

    private final int numVal;

    Type(final int numVal) {
      this.numVal = numVal;
    }

    public int getFlag() {
      return numVal;
    }
  }
}
