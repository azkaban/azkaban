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

import azkaban.flow.Flow;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.Triple;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockProjectLoader implements ProjectLoader {

  public File dir;

  public MockProjectLoader(final File dir) {
    this.dir = dir;
  }

  @Override
  public List<Project> fetchAllActiveProjects() throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Project fetchProjectById(final int id) throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Project createNewProject(final String name, final String description, final User creator)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void removeProject(final Project project, final String user)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updatePermission(final Project project, final String name, final Permission perm,
      final boolean isGroup) throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateDescription(final Project project, final String description, final String user)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean postEvent(final Project project, final EventType type, final String user,
      final String message) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<ProjectLogEvent> getProjectEvents(final Project project, final int num,
      final int skip) throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void uploadProjectFile(final int projectId, final int version, final File localFile,
      final String user)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void addProjectVersion(final int projectId, final int version, final File localFile,
      final String uploader,
      final byte[] md5, final String resourceId)
      throws ProjectManagerException {

  }

  @Override
  public ProjectFileHandler fetchProjectMetaData(final int projectId, final int version) {
    return null;
  }

  @Override
  public ProjectFileHandler getUploadedFile(final int projectId, final int version)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void changeProjectVersion(final Project project, final int version, final String user)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void uploadFlows(final Project project, final int version, final Collection<Flow> flows)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void uploadFlow(final Project project, final int version, final Flow flow)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public Flow fetchFlow(final Project project, final String flowId)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Flow> fetchAllProjectFlows(final Project project)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getLatestProjectVersion(final Project project)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void uploadProjectProperty(final Project project, final Props props)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void uploadProjectProperties(final Project project, final List<Props> properties)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public Props fetchProjectProperty(final Project project, final String propsName)
      throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, Props> fetchProjectProperties(final int projectId, final int version)
      throws ProjectManagerException {
    final Map<String, Props> propertyMap = new HashMap<>();
    for (final File file : this.dir.listFiles()) {
      final String name = file.getName();
      if (name.endsWith(".job") || name.endsWith(".properties")) {
        try {
          final Props props = new Props(null, file);
          propertyMap.put(name, props);
        } catch (final IOException e) {
          throw new ProjectManagerException(e.getMessage());
        }
      }
    }

    return propertyMap;
  }

  @Override
  public void cleanOlderProjectVersion(final int projectId, final int version)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void removePermission(final Project project, final String name, final boolean isGroup)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateProjectProperty(final Project project, final Props props)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public Props fetchProjectProperty(final int projectId, final int projectVer,
      final String propsName) throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Triple<String, Boolean, Permission>> getProjectPermissions(
      final int projectId) throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateProjectSettings(final Project project)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateFlow(final Project project, final int version, final Flow flow)
      throws ProjectManagerException {
    // TODO Auto-generated method stub

  }

  @Override
  public Project fetchProjectByName(final String name) throws ProjectManagerException {
    // TODO Auto-generated method stub
    return null;
  }
}
