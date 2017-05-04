/*
 * Copyright 2014 LinkedIn Corp.
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.project.ProjectLogEvent.EventType;
import azkaban.flow.Flow;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.Triple;
import java.util.concurrent.ConcurrentHashMap;

public class MockProjectLoader implements ProjectLoader {
  public File dir;

  public MockProjectLoader(File dir) {
    this.dir = dir;
  }

  private ConcurrentHashMap<Integer, Project> projectsById =
      new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Project> projectsByName =
      new ConcurrentHashMap<>();

  private static int projectId = 0;

  @Override
  public List<Project> fetchAllActiveProjects() throws ProjectManagerException {
    ArrayList<Project> activeProjects = new ArrayList<>();
    for(Project project : projectsById.values()){
      if(project.isActive()){
        activeProjects.add(project);
      }
    }
    return activeProjects;
  }

  @Override
  public Project fetchProjectById(int id) throws ProjectManagerException {
    System.out.println("MockProjectLoader: fetch project by id " + id);
    if(!projectsById.containsKey(id)){
      throw new ProjectManagerException("Could not get project by id.");
    }
    return projectsById.get(id);
  }

  @Override
  public Project createNewProject(String name, String description, User creator)
      throws ProjectManagerException {
    Project project = new Project(++projectId, name);
    project.setDescription(description);
    project.setActive(true);
    projectsById.put(project.getId(), project);
    projectsByName.put(project.getName(), project);
    System.out.println("MockProjectLoader: Created project " + project.getName() +
        ", id: " + project.getId() + ", description: " + description +
        ", user: " + creator.getUserId());
    return project;
  }

  @Override
  public void removeProject(Project project, String user)
      throws ProjectManagerException {
    project.setActive(false);
    System.out.println("MockProjectLoader: removed project " + project.getName());
  }

  @Override
  public void updatePermission(Project project, String name, Permission perm,
      boolean isGroup) throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateDescription(Project project, String description, String user)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean postEvent(Project project, EventType type, String user,
      String message) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<ProjectLogEvent> getProjectEvents(Project project, int num,
      int skip) throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void uploadProjectFile(int projectId, int version, File localFile, String user)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void addProjectVersion(int projectId, int version, File localFile, String uploader, byte[] md5, String resourceId)
      throws ProjectManagerException {

  }

  @Override
  public ProjectFileHandler fetchProjectMetaData(int projectId, int version) {
    return null;
  }

  @Override
  public ProjectFileHandler getUploadedFile(int projectId, int version)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void changeProjectVersion(Project project, int version, String user)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void uploadFlows(Project project, int version, Collection<Flow> flows)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void uploadFlow(Project project, int version, Flow flow)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public Flow fetchFlow(Project project, String flowId)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Flow> fetchAllProjectFlows(Project project)
      throws ProjectManagerException {
    return new ArrayList<>();
  }

  @Override
  public int getLatestProjectVersion(Project project)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void uploadProjectProperty(Project project, Props props)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void uploadProjectProperties(Project project, List<Props> properties)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public Props fetchProjectProperty(Project project, String propsName)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, Props> fetchProjectProperties(int projectId, int version)
      throws ProjectManagerException {
    Map<String, Props> propertyMap = new HashMap<String, Props>();
    for (File file : dir.listFiles()) {
      String name = file.getName();
      if (name.endsWith(".job") || name.endsWith(".properties")) {
        try {
          Props props = new Props(null, file);
          propertyMap.put(name, props);
        } catch (IOException e) {
          throw new ProjectManagerException(e.getMessage());
        }
      }
    }

    return propertyMap;
  }

  @Override
  public void cleanOlderProjectVersion(int projectId, int version)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void removePermission(Project project, String name, boolean isGroup)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateProjectProperty(Project project, Props props)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public Props fetchProjectProperty(int projectId, int projectVer,
      String propsName) throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Triple<String, Boolean, Permission>> getProjectPermissions(
      int projectId) throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateProjectSettings(Project project)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateFlow(Project project, int version, Flow flow)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public Project fetchProjectByName(String name) throws ProjectManagerException {
    System.out.println("MockProjectLoader: fetch project by name " + name);
    if(!projectsByName.containsKey(name)){
      throw new ProjectManagerException("Could not get project by name.");
    }
    return projectsByName.get(name);
  }
}
