package azkaban.project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.user.Permission;
import azkaban.user.User;

public class Project {
    private final String name;
    private String description;
    private long createTimestamp;
    private long lastModifiedTimestamp;
    private HashMap<String, Permission> userToPermission = new HashMap<String, Permission>();
    
    public Project(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
    
    public void setDescription(String description) {
    	this.description = description;
    }
    
    public String getDescription() {
    	return description;
    }
    
    public void setUserPermission(String userid, Permission flags) {
    	userToPermission.put(userid, flags);
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
    	HashMap<String,Object> projectObject = new HashMap<String, Object>();
    	projectObject.put("name", name);
    	projectObject.put("description", description);
    	projectObject.put("createTimestamp", createTimestamp);
    	projectObject.put("lastModifiedTimestamp",lastModifiedTimestamp);
    	
    	ArrayList<Map<String,Object>> users = new ArrayList<Map<String,Object>>();
    	for (Map.Entry<String, Permission> entry: userToPermission.entrySet()) {
    		HashMap<String,Object> userMap = new HashMap<String,Object>();
    		userMap.put("userid", entry.getKey());
    		userMap.put("permissions", entry.getValue().toStringArray());
    		users.add(userMap);
    	}
 
    	projectObject.put("users", users);
    	return projectObject;
    }
    
    @SuppressWarnings("unchecked")
	public static Project projectFromObject(Object object) {
    	Map<String,Object> projectObject = (Map<String,Object>)object;
    	String name = (String)projectObject.get("name");
    	String description = (String)projectObject.get("description");
    	long createTimestamp = (Long)projectObject.get("createTimestamp");
    	long lastModifiedTimestamp =  (Long)projectObject.get("lastModifiedTimestamp");
    	
    	Project project = new Project(name);
    	project.setDescription(description);
    	project.setCreateTimestamp(createTimestamp);
    	project.setLastModifiedTimestamp(lastModifiedTimestamp);
    	
    	
		List<Map<String,Object>> users = (List<Map<String,Object>>)projectObject.get("users");
    	
    	for (Map<String, Object> user: users) {
    		String userid = (String)user.get("userid");
    		Permission perm = new Permission();
    		List<String> list = (List<String>)user.get("permissions");
    		perm.setPermissions(list);
    		
    		project.setUserPermission(userid, perm);
    	}
    	
    	return null;
    }
}
