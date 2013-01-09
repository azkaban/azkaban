package azkaban.user;

public class Role {
	private final String name;
	private final Permission globalPermission;
	
	public Role(String name, Permission permission) {
		this.name = name;
		this.globalPermission = permission;
	}

	public Permission getPermission() {
		return globalPermission;
	}

	public String getName() {
		return name;
	}
	
	public String toString() {
		return "Role " + name;
	}
}
