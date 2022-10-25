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

import azkaban.ServiceProvider;
import azkaban.event.EventHandler;
import azkaban.flow.Flow;
import azkaban.flow.FlowResourceRecommendation;
import azkaban.spi.AzkabanEventReporter;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Pair;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Project extends EventHandler {

  private final int id;
  private final String name;
  private final LinkedHashMap<String, Permission> userPermissionMap =
      new LinkedHashMap<>();
  private final LinkedHashMap<String, Permission> groupPermissionMap =
      new LinkedHashMap<>();
  private final HashSet<String> proxyUsers = new HashSet<>();
  private boolean active = true;
  private String description;
  private int version = -1;
  private long createTimestamp;
  private long lastModifiedTimestamp;
  private String lastModifiedUser;
  private String source;
  private Map<String, Flow> flows = new HashMap<>();
  // flowResourceRecommendations map shouldn't be ImmutableMap.
  private HashMap<String, FlowResourceRecommendation> flowResourceRecommendations = new HashMap<>();
  private Map<String, Object> metadata = new HashMap<>();
  private static final Logger logger = LoggerFactory.getLogger(Project.class);
  // Added event listener for sending project events
  private final ProjectEventListener projectEventListener= new ProjectEventListener();
  private AzkabanEventReporter azkabanEventReporter;

  public Project(final int id, final String name) {
    this.id = id;
    this.name = name;
    try {
      this.azkabanEventReporter = ServiceProvider.SERVICE_PROVIDER.getInstance(AzkabanEventReporter.class);
    } catch (final Exception e) {
      logger.info("AzkabanEventReporter is not configured");
    } finally {
      // Add the project event listener only if a non-null event reporter is available
      if (this.azkabanEventReporter != null) {
        this.addListener(this.projectEventListener);
      }
    }
  }

  public static Project projectFromObject(final Object object) {
    final Map<String, Object> projectObject = (Map<String, Object>) object;
    final int id = (Integer) projectObject.get("id");
    final String name = (String) projectObject.get("name");
    final String description = (String) projectObject.get("description");
    final String lastModifiedUser = (String) projectObject.get("lastModifiedUser");
    final long createTimestamp = coerceToLong(projectObject.get("createTimestamp"));
    final long lastModifiedTimestamp =
        coerceToLong(projectObject.get("lastModifiedTimestamp"));
    final String source = (String) projectObject.get("source");
    Boolean active = (Boolean) projectObject.get("active");
    active = active == null ? true : active;
    final int version = (Integer) projectObject.get("version");
    final Map<String, Object> metadata =
        (Map<String, Object>) projectObject.get("metadata");

    final Project project = new Project(id, name);
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

    final List<String> proxyUserList = (List<String>) projectObject.get("proxyUsers");
    project.addAllProxyUsers(proxyUserList);

    return project;
  }

  private static long coerceToLong(final Object obj) {
    if (obj == null) {
      return 0;
    } else if (obj instanceof Integer) {
      return (Integer) obj;
    }

    return (Long) obj;
  }

  public String getName() {
    return this.name;
  }

  public Flow getFlow(final String flowId) {
    if (this.flows == null) {
      return null;
    }

    return this.flows.get(flowId);
  }

  public Map<String, Flow> getFlowMap() {
    return this.flows;
  }

  public List<Flow> getFlows() {
    List<Flow> retFlow = null;
    if (this.flows != null) {
      retFlow = new ArrayList<>(this.flows.values());
    } else {
      retFlow = new ArrayList<>();
    }
    return retFlow;
  }

  public void setFlows(final Map<String, Flow> flows) {
    this.flows = ImmutableMap.copyOf(flows);
  }

  public FlowResourceRecommendation getFlowResourceRecommendation(final String flowId) {
    return this.flowResourceRecommendations.get(flowId);
  }

  public Map<String, FlowResourceRecommendation> getFlowResourceRecommendationMap() {
    return this.flowResourceRecommendations;
  }

  public void setFlowResourceRecommendations(@Nonnull final HashMap<String, FlowResourceRecommendation> flowResourceRecommendations) {
    this.flowResourceRecommendations = flowResourceRecommendations;
  }

  public Permission getCollectivePermission(final User user) {
    final Permission permissions = new Permission();
    Permission perm = this.userPermissionMap.get(user.getUserId());
    if (perm != null) {
      permissions.addPermissions(perm);
    }

    for (final String group : user.getGroups()) {
      perm = this.groupPermissionMap.get(group);
      if (perm != null) {
        permissions.addPermissions(perm);
      }
    }

    return permissions;
  }

  public Set<String> getProxyUsers() {
    return new HashSet<>(this.proxyUsers);
  }

  public void addAllProxyUsers(final Collection<String> proxyUsers) {
    this.proxyUsers.addAll(proxyUsers);
  }

  public boolean hasProxyUser(final String proxy) {
    return this.proxyUsers.contains(proxy);
  }

  public void addProxyUser(final String user) {
    this.proxyUsers.add(user);
  }

  public void removeProxyUser(final String user) {
    this.proxyUsers.remove(user);
  }

  public boolean hasPermission(final User user, final Type type) {
    final Permission perm = this.userPermissionMap.get(user.getUserId());
    if (perm != null
        && (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(type))) {
      return true;
    }

    return hasGroupPermission(user, type);
  }

  public boolean hasUserPermission(final User user, final Type type) {
    final Permission perm = this.userPermissionMap.get(user.getUserId());
    if (perm == null) {
      // Check group
      return false;
    }

    if (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(type)) {
      return true;
    }

    return false;
  }

  public boolean hasGroupPermission(final User user, final Type type) {
    for (final String group : user.getGroups()) {
      final Permission perm = this.groupPermissionMap.get(group);
      if (perm != null) {
        if (perm.isPermissionSet(Type.ADMIN) || perm.isPermissionSet(type)) {
          return true;
        }
      }
    }

    return false;
  }

  public List<String> getUsersWithPermission(final Type type) {
    final ArrayList<String> users = new ArrayList<>();
    for (final Map.Entry<String, Permission> entry : this.userPermissionMap.entrySet()) {
      final Permission perm = entry.getValue();
      if (perm.isPermissionSet(type)) {
        users.add(entry.getKey());
      }
    }
    return users;
  }

  public List<Pair<String, Permission>> getUserPermissions() {
    final ArrayList<Pair<String, Permission>> permissions =
        new ArrayList<>();

    for (final Map.Entry<String, Permission> entry : this.userPermissionMap.entrySet()) {
      permissions.add(new Pair<>(entry.getKey(), entry
          .getValue()));
    }

    return permissions;
  }

  public List<Pair<String, Permission>> getGroupPermissions() {
    final ArrayList<Pair<String, Permission>> permissions =
        new ArrayList<>();

    for (final Map.Entry<String, Permission> entry : this.groupPermissionMap.entrySet()) {
      permissions.add(new Pair<>(entry.getKey(), entry
          .getValue()));
    }

    return permissions;
  }

  public String getDescription() {
    return this.description;
  }

  public void setDescription(final String description) {
    this.description = description;
  }

  public void setUserPermission(final String userid, final Permission perm) {
    this.userPermissionMap.put(userid, perm);
  }

  public void setGroupPermission(final String group, final Permission perm) {
    this.groupPermissionMap.put(group, perm);
  }

  public Permission getUserPermission(final User user) {
    return this.userPermissionMap.get(user.getUserId());
  }

  public Permission getGroupPermission(final String group) {
    return this.groupPermissionMap.get(group);
  }

  public Permission getUserPermission(final String userID) {
    return this.userPermissionMap.get(userID);
  }

  public void removeGroupPermission(final String group) {
    this.groupPermissionMap.remove(group);
  }

  public void removeUserPermission(final String userId) {
    this.userPermissionMap.remove(userId);
  }

  public void clearUserPermission() {
    this.userPermissionMap.clear();
  }

  public long getCreateTimestamp() {
    return this.createTimestamp;
  }

  public void setCreateTimestamp(final long createTimestamp) {
    this.createTimestamp = createTimestamp;
  }

  public long getLastModifiedTimestamp() {
    return this.lastModifiedTimestamp;
  }

  public void setLastModifiedTimestamp(final long lastModifiedTimestamp) {
    this.lastModifiedTimestamp = lastModifiedTimestamp;
  }

  public Object toObject() {
    final HashMap<String, Object> projectObject = new HashMap<>();
    projectObject.put("id", this.id);
    projectObject.put("name", this.name);
    projectObject.put("description", this.description);
    projectObject.put("createTimestamp", this.createTimestamp);
    projectObject.put("lastModifiedTimestamp", this.lastModifiedTimestamp);
    projectObject.put("lastModifiedUser", this.lastModifiedUser);
    projectObject.put("version", this.version);

    if (!this.active) {
      projectObject.put("active", false);
    }

    if (this.source != null) {
      projectObject.put("source", this.source);
    }

    if (this.metadata != null) {
      projectObject.put("metadata", this.metadata);
    }

    final ArrayList<String> proxyUserList = new ArrayList<>(this.proxyUsers);
    projectObject.put("proxyUsers", proxyUserList);

    return projectObject;
  }

  public String getLastModifiedUser() {
    return this.lastModifiedUser;
  }

  public void setLastModifiedUser(final String lastModifiedUser) {
    this.lastModifiedUser = lastModifiedUser;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (this.active ? 1231 : 1237);
    result =
        prime * result + (int) (this.createTimestamp ^ (this.createTimestamp >>> 32));
    result =
        prime * result + ((this.description == null) ? 0 : this.description.hashCode());
    result = prime * result + this.id;
    result =
        prime * result
            + (int) (this.lastModifiedTimestamp ^ (this.lastModifiedTimestamp >>> 32));
    result =
        prime * result
            + ((this.lastModifiedUser == null) ? 0 : this.lastModifiedUser.hashCode());
    result = prime * result + ((this.name == null) ? 0 : this.name.hashCode());
    result = prime * result + ((this.source == null) ? 0 : this.source.hashCode());
    result = prime * result + this.version;
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
    final Project other = (Project) obj;
    if (this.active != other.active) {
      return false;
    }
    if (this.createTimestamp != other.createTimestamp) {
      return false;
    }
    if (this.description == null) {
      if (other.description != null) {
        return false;
      }
    } else if (!this.description.equals(other.description)) {
      return false;
    }
    if (this.id != other.id) {
      return false;
    }
    if (this.lastModifiedTimestamp != other.lastModifiedTimestamp) {
      return false;
    }
    if (this.lastModifiedUser == null) {
      if (other.lastModifiedUser != null) {
        return false;
      }
    } else if (!this.lastModifiedUser.equals(other.lastModifiedUser)) {
      return false;
    }
    if (this.name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!this.name.equals(other.name)) {
      return false;
    }
    if (this.source == null) {
      if (other.source != null) {
        return false;
      }
    } else if (!this.source.equals(other.source)) {
      return false;
    }
    if (this.version != other.version) {
      return false;
    }
    return true;
  }

  public String getSource() {
    return this.source;
  }

  public void setSource(final String source) {
    this.source = source;
  }

  public Map<String, Object> getMetadata() {
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    return this.metadata;
  }

  protected void setMetadata(final Map<String, Object> metadata) {
    this.metadata = metadata;
  }

  public int getId() {
    return this.id;
  }

  public boolean isActive() {
    return this.active;
  }

  public void setActive(final boolean active) {
    this.active = active;
  }

  public int getVersion() {
    return this.version;
  }

  public void setVersion(final int version) {
    this.version = version;
  }

  public AzkabanEventReporter getAzkabanEventReporter(){
    return this.azkabanEventReporter;
  }
}
