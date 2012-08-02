package azkaban.user;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import azkaban.utils.Utils;

public class Permission {
	public enum Type {
		READ,
		WRITE,
		EXECUTE,
		SCHEDULE,
		ADMIN
	}

	private Set<Type> permissions = new HashSet<Type>();
	
	public Permission() {
	}
	
	public Permission(Type ... list) {
		addPermission(list);
	}
	
	public void setPermission(Type type, boolean set) {
		if (set) {
			addPermission(type);
		}
		else {
			removePermissions(type);
		}
	}
	
	public void addPermission(Type ... list) {
		// Admin is all encompassing permission. No need to add other types
		if (!permissions.contains(Type.ADMIN)) {
			for (Type perm: list) {
				permissions.add(perm);
			}
			// We add everything, and if there's Admin left, we make sure that only Admin is remaining.
			if (permissions.contains(Type.ADMIN)) {
				permissions.clear();
				permissions.add(Type.ADMIN);
			}
		}
	}
	
	public void addPermissionsByName(String ... list) {
		for (String perm: list) {
			Type type = Type.valueOf(perm);
			if (type != null) {
				addPermission(type);
			};
		}
	}
	
	public void addPermissions(Collection<Type> list) {
		for (Type perm: list) {
			addPermission(perm);
		}
	}
	
	public void addPermissionsByName(Collection<String> list) {
		for (String perm: list) {
			Type type = Type.valueOf(perm);
			if (type != null) {
				addPermission(type);
			};
		}
	}
	
	public void removePermissions(Type ... list) {
		for (Type perm: list) {
			permissions.remove(perm);
		}
	}
	
	public void removePermissionsByName(String ... list) {
		for (String perm: list) {
			Type type = Type.valueOf(perm);
			if (type != null) {
				permissions.remove(type);
			};
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
		for (Type type: permissions) {
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
		result = prime * result
				+ ((permissions == null) ? 0 : permissions.hashCode());
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
}
