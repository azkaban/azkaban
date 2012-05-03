package azkaban.user;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class User {
	private final String userid;
	private Set<String> roles = new HashSet<String>();
	private Set<String> groups = new HashSet<String>();
	
	public User(String userid) {
		this.userid = userid;
	}
	
	public String getUserId() {
		return userid;
	}

	public List<String> getGroups() {
		return new ArrayList<String>(groups);
	}

	public void clearGroup() {
		groups.clear();
	}
	
	public void addGroup(String name) {
		groups.add(name);
	}
	
	public boolean isInGroup(String group) {
		return this.groups.contains(group);
	}
	
	public List<String> getRoles() {
		return new ArrayList<String>(roles);
	}
	
	public void addRole(String role) {
		this.roles.add(role);
	}
	
	public boolean hasRole(String role) {
		return roles.contains(role);
	}
	
	public String toString() {
		String groupStr = "[";
		for (String group: groups) {
			groupStr += group + ",";
		}
		groupStr += "]";
		return userid + ": " + groupStr;
	}
}
