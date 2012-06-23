package azkaban.project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;

public class Project {
	private final String name;
	private String description;
	private long createTimestamp;
	private long lastModifiedTimestamp;
	private String lastModifiedUser;
	private String source;
	private HashMap<String, Permission> userToPermission = new HashMap<String, Permission>();
	
	public Project(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public boolean hasPermission(User user, Type type) {
		Permission perm = userToPermission.get(user.getUserId());
		if (perm == null) {
			return false;
		}

		if (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(type)) {
			return true;
		}

		return false;
	}

	public List<String> getUsersWithPermission(Type type) {
		ArrayList<String> users = new ArrayList<String>();
		for (Map.Entry<String, Permission> entry : userToPermission.entrySet()) {
			Permission perm = entry.getValue();
			if (perm.isPermissionSet(type)) {
				users.add(entry.getKey());
			}
		}
		return users;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDescription() {
		return description;
	}

	public void setUserPermission(String userid, Permission perm) {
		userToPermission.put(userid, perm);
	}

	public Permission getUserPermission(User user) {
		return userToPermission.get(user.getUserId());
	}

	public long getCreateTimestamp() {
		return createTimestamp;
	}

	public void setCreateTimestamp(long createTimestamp) {
		this.createTimestamp = createTimestamp;
	}

	public long getLastModifiedTimestamp() {
		return lastModifiedTimestamp;
	}

	public void setLastModifiedTimestamp(long lastModifiedTimestamp) {
		this.lastModifiedTimestamp = lastModifiedTimestamp;
	}

	public Object toObject() {
		HashMap<String, Object> projectObject = new HashMap<String, Object>();
		projectObject.put("name", name);
		projectObject.put("description", description);
		projectObject.put("createTimestamp", createTimestamp);
		projectObject.put("lastModifiedTimestamp", lastModifiedTimestamp);
		projectObject.put("lastModifiedUser", lastModifiedUser);
		
		if (source != null) {
			projectObject.put("source", source);
		}

		ArrayList<Map<String, Object>> users = new ArrayList<Map<String, Object>>();
		for (Map.Entry<String, Permission> entry : userToPermission.entrySet()) {
			HashMap<String, Object> userMap = new HashMap<String, Object>();
			userMap.put("userid", entry.getKey());
			userMap.put("permissions", entry.getValue().toStringArray());
			users.add(userMap);
		}

		projectObject.put("users", users);
		return projectObject;
	}

	@SuppressWarnings("unchecked")
	public static Project projectFromObject(Object object) {
		Map<String, Object> projectObject = (Map<String, Object>) object;
		String name = (String) projectObject.get("name");
		String description = (String) projectObject.get("description");
		String lastModifiedUser = (String) projectObject
				.get("lastModifiedUser");
		long createTimestamp = coerceToLong(projectObject
				.get("createTimestamp"));
		long lastModifiedTimestamp = coerceToLong(projectObject
				.get("lastModifiedTimestamp"));
		String source = (String)projectObject.get("source");
		
		Project project = new Project(name);
		project.setDescription(description);
		project.setCreateTimestamp(createTimestamp);
		project.setLastModifiedTimestamp(lastModifiedTimestamp);
		project.setLastModifiedUser(lastModifiedUser);

		if (source != null) {
			project.setSource(source);
		}

		
		List<Map<String, Object>> users = (List<Map<String, Object>>) projectObject
				.get("users");

		for (Map<String, Object> user : users) {
			String userid = (String) user.get("userid");
			Permission perm = new Permission();
			List<String> list = (List<String>) user.get("permissions");
			perm.setPermissionsByName(list);

			project.setUserPermission(userid, perm);
		}

		return project;
	}

	private static long coerceToLong(Object obj) {
		if (obj == null) {
			return 0;
		} else if (obj instanceof Integer) {
			return (Integer) obj;
		}

		return (Long) obj;
	}

	public String getLastModifiedUser() {
		return lastModifiedUser;
	}

	public void setLastModifiedUser(String lastModifiedUser) {
		this.lastModifiedUser = lastModifiedUser;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ (int) (createTimestamp ^ (createTimestamp >>> 32));
		result = prime * result
				+ ((description == null) ? 0 : description.hashCode());
		result = prime
				* result
				+ (int) (lastModifiedTimestamp ^ (lastModifiedTimestamp >>> 32));
		result = prime
				* result
				+ ((lastModifiedUser == null) ? 0 : lastModifiedUser.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime
				* result
				+ ((userToPermission == null) ? 0 : userToPermission.hashCode());
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
		Project other = (Project) obj;
		if (createTimestamp != other.createTimestamp)
			return false;
		if (description == null) {
			if (other.description != null)
				return false;
		} else if (!description.equals(other.description))
			return false;
		if (lastModifiedTimestamp != other.lastModifiedTimestamp)
			return false;
		if (lastModifiedUser == null) {
			if (other.lastModifiedUser != null)
				return false;
		} else if (!lastModifiedUser.equals(other.lastModifiedUser))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (userToPermission == null) {
			if (other.userToPermission != null)
				return false;
		} else if (!userToPermission.equals(other.userToPermission))
			return false;
		return true;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
}
