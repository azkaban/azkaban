package azkaban.user;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Permission {
	public static final String[] PERMISSIONS = {"READ", "WRITE", "EXECUTE", "SCHEDULE", "DELETE", "ADMIN"};
	private static final Map<String, Integer> PERMISSIONS_TO_INDEX = new HashMap<String,Integer>();
	
	static {
		int i = 0;
		for (String perm: PERMISSIONS) {
			PERMISSIONS_TO_INDEX.put(perm, i++);
		}
	}
	
	private boolean[] flags;
	
	public Permission() {
		flags = new boolean[PERMISSIONS.length];
	}
	
	public void setPermissions(String ... list) {
		for (String perm: list) {
			Integer index = PERMISSIONS_TO_INDEX.get(perm);
			if (index != null) {
				flags[index] = true;
			}
		}
	}
	
	public void setPermissions(Collection<String> list) {
		for (String perm: list) {
			Integer index = PERMISSIONS_TO_INDEX.get(perm);
			if (index != null) {
				flags[index] = true;
			}
		}
	}
	
	public void unsetPermissions(String ... list) {
		for (String perm: list) {
			Integer index = PERMISSIONS_TO_INDEX.get(perm);
			if (index != null) {
				flags[index] = false;
			}
		}
	}
	
	public boolean isPermissionSet(String permission) {
		Integer index = PERMISSIONS_TO_INDEX.get(permission);
		if (index != null) {
			return flags[index];
		}

		return false;
	}
	
	public String[] toStringArray() {
		ArrayList<String> list = new ArrayList<String>();
		int count = 0;
		for (int i = 0; i < flags.length; ++i) {
			if (flags[i]) {
				list.add(PERMISSIONS[i]);
				count++;
			}
		}
		return list.toArray(new String[count]);
	}
}
