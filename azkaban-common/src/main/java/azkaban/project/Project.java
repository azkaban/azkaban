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

package azkaban.project;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import azkaban.flow.Flow;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Pair;

public class Project {
  private final int id;
  private final String name;
  private boolean active = true;
  private String description;
  private int version = -1;
  private long createTimestamp;
  private long lastModifiedTimestamp;
  private String lastModifiedUser;
  private String source;
  private LinkedHashMap<String, Permission> userPermissionMap =
      new LinkedHashMap<String, Permission>();
  private LinkedHashMap<String, Permission> groupPermissionMap =
      new LinkedHashMap<String, Permission>();
  private Map<String, Flow> flows = null;
  private HashSet<String> proxyUsers = new HashSet<String>();
  private Map<String, Object> metadata = new HashMap<String, Object>();

  public Project(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setFlows(Map<String, Flow> flows) {
    this.flows = flows;
  }

  public Flow getFlow(String flowId) {
    if (flows == null) {
      return null;
    }

    return flows.get(flowId);
  }

  public Map<String, Flow> getFlowMap() {
    return flows;
  }

  public List<Flow> getFlows() {
    List<Flow> retFlow = null;
    if (flows != null) {
      retFlow = new ArrayList<Flow>(flows.values());
    } else {
      retFlow = new ArrayList<Flow>();
    }
    return retFlow;
  }

  public Permission getCollectivePermission(User user) {
    Permission permissions = new Permission();
    Permission perm = userPermissionMap.get(user.getUserId());
    if (perm != null) {
      permissions.addPermissions(perm);
    }

    for (String group : user.getGroups()) {
      perm = groupPermissionMap.get(group);
      if (perm != null) {
        permissions.addPermissions(perm);
      }
    }

    return permissions;
  }

  public Set<String> getProxyUsers() {
    return new HashSet<String>(proxyUsers);
  }

  public void addAllProxyUsers(Collection<String> proxyUsers) {
    this.proxyUsers.addAll(proxyUsers);
  }

  public boolean hasProxyUser(String proxy) {
    return this.proxyUsers.contains(proxy);
  }

  public void addProxyUser(String user) {
    this.proxyUsers.add(user);
  }

  public void removeProxyUser(String user) {
    this.proxyUsers.remove(user);
  }

  public boolean hasPermission(User user, Type type) {
    Permission perm = userPermissionMap.get(user.getUserId());
    if (perm != null
        && (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(type))) {
      return true;
    }

    return hasGroupPermission(user, type);
  }

  public boolean hasUserPermission(User user, Type type) {
    Permission perm = userPermissionMap.get(user.getUserId());
    if (perm == null) {
      // Check group
      return false;
    }

    if (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(type)) {
      return true;
    }

    return false;
  }

  public boolean hasGroupPermission(User user, Type type) {
    for (String group : user.getGroups()) {
      Permission perm = groupPermissionMap.get(group);
      if (perm != null) {
        if (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(type)) {
          return true;
        }
      }
    }

    return false;
  }

  public List<String> getUsersWithPermission(Type type) {
    ArrayList<String> users = new ArrayList<String>();
    for (Map.Entry<String, Permission> entry : userPermissionMap.entrySet()) {
      Permission perm = entry.getValue();
      if (perm.isPermissionSet(type)) {
        users.add(entry.getKey());
      }
    }
    return users;
  }

  public List<Pair<String, Permission>> getUserPermissions() {
    ArrayList<Pair<String, Permission>> permissions =
        new ArrayList<Pair<String, Permission>>();

    for (Map.Entry<String, Permission> entry : userPermissionMap.entrySet()) {
      permissions.add(new Pair<String, Permission>(entry.getKey(), entry
          .getValue()));
    }

    return permissions;
  }

  public List<Pair<String, Permission>> getGroupPermissions() {
    ArrayList<Pair<String, Permission>> permissions =
        new ArrayList<Pair<String, Permission>>();

    for (Map.Entry<String, Permission> entry : groupPermissionMap.entrySet()) {
      permissions.add(new Pair<String, Permission>(entry.getKey(), entry
          .getValue()));
    }

    return permissions;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public void setUserPermission(String userid, Permission perm) {
    userPermissionMap.put(userid, perm);
  }

  public void setGroupPermission(String group, Permission perm) {
    groupPermissionMap.put(group, perm);
  }

  public Permission getUserPermission(User user) {
    return userPermissionMap.get(user.getUserId());
  }

  public Permission getGroupPermission(String group) {
    return groupPermissionMap.get(group);
  }

  public Permission getUserPermission(String userID) {
    return userPermissionMap.get(userID);
  }

  public void removeGroupPermission(String group) {
    groupPermissionMap.remove(group);
  }

  public void removeUserPermission(String userId) {
    userPermissionMap.remove(userId);
  }

  public void clearUserPermission() {
    userPermissionMap.clear();
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
    projectObject.put("id", id);
    projectObject.put("name", name);
    projectObject.put("description", description);
    projectObject.put("createTimestamp", createTimestamp);
    projectObject.put("lastModifiedTimestamp", lastModifiedTimestamp);
    projectObject.put("lastModifiedUser", lastModifiedUser);
    projectObject.put("version", version);

    if (!active) {
      projectObject.put("active", false);
    }

    if (source != null) {
      projectObject.put("source", source);
    }

    if (metadata != null) {
      projectObject.put("metadata", metadata);
    }

    ArrayList<String> proxyUserList = new ArrayList<String>(proxyUsers);
    projectObject.put("proxyUsers", proxyUserList);

    return projectObject;
  }

  @SuppressWarnings("unchecked")
  public static Project projectFromObject(Object object) {
    Map<String, Object> projectObject = (Map<String, Object>) object;
    int id = (Integer) projectObject.get("id");
    String name = (String) projectObject.get("name");
    String description = (String) projectObject.get("description");
    String lastModifiedUser = (String) projectObject.get("lastModifiedUser");
    long createTimestamp = coerceToLong(projectObject.get("createTimestamp"));
    long lastModifiedTimestamp =
        coerceToLong(projectObject.get("lastModifiedTimestamp"));
    String source = (String) projectObject.get("source");
    Boolean active = (Boolean) projectObject.get("active");
    active = active == null ? true : active;
    int version = (Integer) projectObject.get("version");
    Map<String, Object> metadata =
        (Map<String, Object>) projectObject.get("metadata");

    Project project = new Project(id, name);
    project.setVersion(version);
    project.setDescription(description);
    project.setCreateTimestamp(createTimestamp);
    project.setLastModifiedTimestamp(lastModifiedTimestamp);
    project.setLastModifiedUser(lastModifiedUser);
    project.setActive(active);

    if (source != null) {
      project.setSource(source);
    }
    if (metadata != null) {
      project.setMetadata(metadata);
    }

    List<String> proxyUserList = (List<String>) projectObject.get("proxyUsers");
    project.addAllProxyUsers(proxyUserList);

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
    result = prime * result + (active ? 1231 : 1237);
    result =
        prime * result + (int) (createTimestamp ^ (createTimestamp >>> 32));
    result =
        prime * result + ((description == null) ? 0 : description.hashCode());
    result = prime * result + id;
    result =
        prime * result
            + (int) (lastModifiedTimestamp ^ (lastModifiedTimestamp >>> 32));
    result =
        prime * result
            + ((lastModifiedUser == null) ? 0 : lastModifiedUser.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((source == null) ? 0 : source.hashCode());
    result = prime * result + version;
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
    if (active != other.active)
      return false;
    if (createTimestamp != other.createTimestamp)
      return false;
    if (description == null) {
      if (other.description != null)
        return false;
    } else if (!description.equals(other.description))
      return false;
    if (id != other.id)
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
    if (source == null) {
      if (other.source != null)
        return false;
    } else if (!source.equals(other.source))
      return false;
    if (version != other.version)
      return false;
    return true;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public Map<String, Object> getMetadata() {
    if (metadata == null) {
      metadata = new HashMap<String, Object>();
    }
    return metadata;
  }

  protected void setMetadata(Map<String, Object> metadata) {
    this.metadata = metadata;
  }

  public int getId() {
    return id;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
