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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import azkaban.utils.Utils;

public class Permission {
  public enum Type {
    READ(0x0000001),
    WRITE(0x0000002),
    EXECUTE(0x0000004),
    SCHEDULE(0x0000008),
    METRICS(0x0000010),
    CREATEPROJECTS(0x40000000), // Only used for roles
    ADMIN(0x8000000);

    private int numVal;

    Type(int numVal) {
      this.numVal = numVal;
    }

    public int getFlag() {
      return numVal;
    }
  }

  private Set<Type> permissions = new HashSet<Type>();

  public Permission() {
  }

  public Permission(int flags) {
    setPermissions(flags);
  }

  public Permission(Type... list) {
    addPermission(list);
  }

  public void addPermissions(Permission perm) {
    this.permissions.addAll(perm.getTypes());
  }

  public void setPermission(Type type, boolean set) {
    if (set) {
      addPermission(type);
    } else {
      removePermissions(type);
    }
  }

  public void setPermissions(int flags) {
    permissions.clear();
    if ((flags & Type.ADMIN.getFlag()) != 0) {
      addPermission(Type.ADMIN);
    } else {
      for (Type type : Type.values()) {
        if ((flags & type.getFlag()) != 0) {
          addPermission(type);
        }
      }
    }
  }

  public void addPermission(Type... list) {
    // Admin is all encompassing permission. No need to add other types
    if (!permissions.contains(Type.ADMIN)) {
      for (Type perm : list) {
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

  public void addPermissionsByName(String... list) {
    for (String perm : list) {
      Type type = Type.valueOf(perm);
      if (type != null) {
        addPermission(type);
      }
      ;
    }
  }

  public void addPermissions(Collection<Type> list) {
    for (Type perm : list) {
      addPermission(perm);
    }
  }

  public void addPermissionsByName(Collection<String> list) {
    for (String perm : list) {
      Type type = Type.valueOf(perm);
      if (type != null) {
        addPermission(type);
      }
      ;
    }
  }

  public Set<Type> getTypes() {
    return permissions;
  }

  public void removePermissions(Type... list) {
    for (Type perm : list) {
      permissions.remove(perm);
    }
  }

  public void removePermissionsByName(String... list) {
    for (String perm : list) {
      Type type = Type.valueOf(perm);
      if (type != null) {
        permissions.remove(type);
      }
      ;
    }
  }

  public boolean isPermissionSet(Type permission) {
    return permissions.contains(permission);
  }

  public boolean isPermissionNameSet(String permission) {
    return permissions.contains(Type.valueOf(permission));
  }

  public String[] toStringArray() {
    ArrayList<String> list = new ArrayList<String>();
    int count = 0;
    for (Type type : permissions) {
      list.add(type.toString());
      count++;
    }

    return list.toArray(new String[count]);
  }

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
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Permission other = (Permission) obj;
    if (permissions == null) {
      if (other.permissions != null)
        return false;
    } else if (!permissions.equals(other.permissions))
      return false;
    return true;
  }

  public int toFlags() {
    int flag = 0;
    for (Type type : permissions) {
      flag |= type.getFlag();
    }

    return flag;
  }
}
