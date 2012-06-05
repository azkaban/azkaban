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
		setPermission(list);
	}
	
	public void setPermission(Type ... list) {
		for (Type perm: list) {
			permissions.add(perm);
		}
	}
	
	public void setPermissionsByName(String ... list) {
		for (String perm: list) {
			Type type = Type.valueOf(perm);
			if (type != null) {
				permissions.add(type);
			};
		}
	}
	
	public void setPermissions(Collection<Type> list) {
		for (Type perm: list) {
			permissions.add(perm);
		}
	}
	
	public void setPermissionsByName(Collection<String> list) {
		for (String perm: list) {
			Type type = Type.valueOf(perm);
			if (type != null) {
				permissions.add(type);
			};
		}
	}
	
	public void unsetPermissions(Type ... list) {
		for (Type perm: list) {
			permissions.remove(perm);
		}
	}
	
	public void unsetPermissionsByName(String ... list) {
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
