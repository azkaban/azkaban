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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import azkaban.database.AbstractJdbcLoader;
import azkaban.flow.Flow;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Md5Hasher;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Triple;

public class JdbcProjectLoader extends AbstractJdbcLoader implements
    ProjectLoader {
  private static final Logger logger = Logger
      .getLogger(JdbcProjectLoader.class);

  private static final int CHUCK_SIZE = 1024 * 1024 * 10;
  private File tempDir;

  private EncodingType defaultEncodingType = EncodingType.GZIP;

  public JdbcProjectLoader(Props props) {
    super(props);
    tempDir = new File(props.getString("project.temp.dir", "temp"));
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
  }

  @Override
  public List<Project> fetchAllActiveProjects() throws ProjectManagerException {
    Connection connection = getConnection();

    List<Project> projects = null;
    try {
      projects = fetchAllActiveProjects(connection);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return projects;
  }

  private List<Project> fetchAllActiveProjects(Connection connection)
      throws ProjectManagerException {
    QueryRunner runner = new QueryRunner();

    ProjectResultHandler handler = new ProjectResultHandler();
    List<Project> projects = null;
    try {
      projects =
          runner.query(connection,
              ProjectResultHandler.SELECT_ALL_ACTIVE_PROJECTS, handler);

      for (Project project : projects) {
        List<Triple<String, Boolean, Permission>> permissions =
            fetchPermissionsForProject(connection, project);

        for (Triple<String, Boolean, Permission> entry : permissions) {
          if (entry.getSecond()) {
            project.setGroupPermission(entry.getFirst(), entry.getThird());
          } else {
            project.setUserPermission(entry.getFirst(), entry.getThird());
          }
        }
      }
    } catch (SQLException e) {
      throw new ProjectManagerException("Error retrieving all projects", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return projects;
  }

  @Override
  public Project fetchProjectById(int id) throws ProjectManagerException {
    Connection connection = getConnection();

    Project project = null;
    try {
      project = fetchProjectById(connection, id);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return project;
  }

  private Project fetchProjectById(Connection connection, int id)
      throws ProjectManagerException {
    QueryRunner runner = new QueryRunner();
    // Fetch the project
    Project project = null;
    ProjectResultHandler handler = new ProjectResultHandler();
    try {
      List<Project> projects =
          runner.query(connection, ProjectResultHandler.SELECT_PROJECT_BY_ID,
              handler, id);
      if (projects.isEmpty()) {
        throw new ProjectManagerException("No project with id " + id
            + " exists in db.");
      }

      project = projects.get(0);
    } catch (SQLException e) {
      logger.error(ProjectResultHandler.SELECT_PROJECT_BY_ID + " failed.");
      throw new ProjectManagerException(
          "Query for existing project failed. Project " + id, e);
    }

    // Fetch the user permissions
    List<Triple<String, Boolean, Permission>> permissions =
        fetchPermissionsForProject(connection, project);

    for (Triple<String, Boolean, Permission> perm : permissions) {
      if (perm.getThird().toFlags() != 0) {
        if (perm.getSecond()) {
          project.setGroupPermission(perm.getFirst(), perm.getThird());
        } else {
          project.setUserPermission(perm.getFirst(), perm.getThird());
        }
      }
    }

    return project;
  }

    /**
     * Fetch first project with a given name {@inheritDoc}
     *
     * @see azkaban.project.ProjectLoader#fetchProjectByName(java.lang.String)
     */
    @Override
    public Project fetchProjectByName(String name)
        throws ProjectManagerException {
        Connection connection = getConnection();

        Project project = null;
        try {
            project = fetchProjectByName(connection, name);
        } finally {
            DbUtils.closeQuietly(connection);
        }

        return project;
    }

    private Project fetchProjectByName(Connection connection, String name)
        throws ProjectManagerException {
        QueryRunner runner = new QueryRunner();
        // Fetch the project
        Project project = null;
        ProjectResultHandler handler = new ProjectResultHandler();
        try {
            List<Project> projects =
                runner.query(connection,
                    ProjectResultHandler.SELECT_PROJECT_BY_NAME, handler, name);
            if (projects.isEmpty()) {
                throw new ProjectManagerException(
                    "No project with name " + name + " exists in db.");
            }

            project = projects.get(0);
        } catch (SQLException e) {
            logger.error(ProjectResultHandler.SELECT_PROJECT_BY_NAME
                + " failed.");
            throw new ProjectManagerException(
                "Query for existing project failed. Project " + name, e);
        }

        // Fetch the user permissions
        List<Triple<String, Boolean, Permission>> permissions =
            fetchPermissionsForProject(connection, project);

        for (Triple<String, Boolean, Permission> perm : permissions) {
            if (perm.getThird().toFlags() != 0) {
                if (perm.getSecond()) {
                    project
                        .setGroupPermission(perm.getFirst(), perm.getThird());
                } else {
                    project.setUserPermission(perm.getFirst(), perm.getThird());
                }
            }
        }

        return project;
    }

  private List<Triple<String, Boolean, Permission>> fetchPermissionsForProject(
      Connection connection, Project project) throws ProjectManagerException {
    ProjectPermissionsResultHandler permHander =
        new ProjectPermissionsResultHandler();

    QueryRunner runner = new QueryRunner();
    List<Triple<String, Boolean, Permission>> permissions = null;
    try {
      permissions =
          runner.query(connection,
              ProjectPermissionsResultHandler.SELECT_PROJECT_PERMISSION,
              permHander, project.getId());
    } catch (SQLException e) {
      throw new ProjectManagerException("Query for permissions for "
          + project.getName() + " failed.", e);
    }

    return permissions;
  }

  /**
   * Creates a Project in the db.
   *
   * It will throw an exception if it finds an active project of the same name,
   * or the SQL fails
   */
  @Override
  public Project createNewProject(String name, String description, User creator)
      throws ProjectManagerException {
    Connection connection = getConnection();

    Project project;
    try {
      // No need to commit, since createNewProject should commit.
      project = createNewProject(connection, name, description, creator);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return project;
  }

  private synchronized Project createNewProject(Connection connection,
      String name, String description, User creator)
      throws ProjectManagerException {
    QueryRunner runner = new QueryRunner();
    ProjectResultHandler handler = new ProjectResultHandler();

    // See if it exists first.
    try {
      List<Project> project =
          runner
              .query(connection,
                  ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME, handler,
                  name);
      if (!project.isEmpty()) {
        throw new ProjectManagerException("Active project with name " + name
            + " already exists in db.");
      }
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Checking for existing project failed. " + name, e);
    }

    final String INSERT_PROJECT =
        "INSERT INTO projects ( name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob) values (?,?,?,?,?,?,?,?,?)";
    // Insert project
    try {
      long time = System.currentTimeMillis();
      int i =
          runner.update(connection, INSERT_PROJECT, name, true, time, time,
              null, creator.getUserId(), description,
              defaultEncodingType.getNumVal(), null);
      if (i == 0) {
        throw new ProjectManagerException("No projects have been inserted.");
      }
      connection.commit();

    } catch (SQLException e) {
      logger.error(INSERT_PROJECT + " failed.");
      try {
        connection.rollback();
      } catch (SQLException e1) {
        e1.printStackTrace();
      }
      throw new ProjectManagerException(
          "Insert project for existing project failed. " + name, e);
    }

    // Do another query to grab and return the project.
    Project project = null;
    try {
      List<Project> projects =
          runner
              .query(connection,
                  ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME, handler,
                  name);
      if (projects.isEmpty()) {
        throw new ProjectManagerException("No active project with name " + name
            + " exists in db.");
      } else if (projects.size() > 1) {
        throw new ProjectManagerException("More than one active project "
            + name);
      }

      project = projects.get(0);
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Checking for existing project failed. " + name, e);
    }

    return project;
  }

  @Override
  public void uploadProjectFile(Project project, int version, String filetype,
      String filename, File localFile, String uploader)
      throws ProjectManagerException {
    logger.info("Uploading to " + project.getName() + " version:" + version
        + " file:" + filename);
    Connection connection = getConnection();

    try {
      uploadProjectFile(connection, project, version, filetype, filename,
          localFile, uploader);
      connection.commit();
      logger.info("Commiting upload " + localFile.getName());
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error getting DB connection.", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void uploadProjectFile(Connection connection, Project project,
      int version, String filetype, String filename, File localFile,
      String uploader) throws ProjectManagerException {
    QueryRunner runner = new QueryRunner();
    long updateTime = System.currentTimeMillis();

    logger.info("Creating message digest for upload " + localFile.getName());
    byte[] md5 = null;
    try {
      md5 = Md5Hasher.md5Hash(localFile);
    } catch (IOException e) {
      throw new ProjectManagerException("Error getting md5 hash.", e);
    }

    logger.info("Md5 hash created");
    // Really... I doubt we'll get a > 2gig file. So int casting it is!
    byte[] buffer = new byte[CHUCK_SIZE];
    final String INSERT_PROJECT_FILES =
        "INSERT INTO project_files (project_id, version, chunk, size, file) values (?,?,?,?,?)";

    BufferedInputStream bufferedStream = null;
    int chunk = 0;
    try {
      bufferedStream = new BufferedInputStream(new FileInputStream(localFile));
      int size = bufferedStream.read(buffer);
      while (size >= 0) {
        logger.info("Read bytes for " + filename + " size:" + size);
        byte[] buf = buffer;
        if (size < buffer.length) {
          buf = Arrays.copyOfRange(buffer, 0, size);
        }
        try {
          logger.info("Running update for " + filename + " chunk " + chunk);
          runner.update(connection, INSERT_PROJECT_FILES, project.getId(),
              version, chunk, size, buf);
          logger.info("Finished update for " + filename + " chunk " + chunk);
        } catch (SQLException e) {
          throw new ProjectManagerException("Error chunking", e);
        }
        ++chunk;

        size = bufferedStream.read(buffer);
      }
    } catch (IOException e) {
      throw new ProjectManagerException("Error chunking file " + filename);
    } finally {
      IOUtils.closeQuietly(bufferedStream);
    }

    final String INSERT_PROJECT_VERSION =
        "INSERT INTO project_versions (project_id, version, upload_time, uploader, file_type, file_name, md5, num_chunks) values (?,?,?,?,?,?,?,?)";

    try {
      runner.update(connection, INSERT_PROJECT_VERSION, project.getId(),
          version, updateTime, uploader, filetype, filename, md5, chunk);
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error updating project version "
          + project.getName(), e);
    }
  }

  @Override
  public ProjectFileHandler getUploadedFile(Project project, int version)
      throws ProjectManagerException {
    logger.info("Retrieving to " + project.getName() + " version:" + version);
    Connection connection = getConnection();
    ProjectFileHandler handler = null;
    try {
      handler = getUploadedFile(connection, project.getId(), version);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return handler;
  }

  @Override
  public ProjectFileHandler getUploadedFile(int projectId, int version)
      throws ProjectManagerException {
    logger.info("Retrieving to " + projectId + " version:" + version);
    Connection connection = getConnection();
    ProjectFileHandler handler = null;
    try {
      handler = getUploadedFile(connection, projectId, version);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return handler;
  }

  private ProjectFileHandler getUploadedFile(Connection connection,
      int projectId, int version) throws ProjectManagerException {
    QueryRunner runner = new QueryRunner();
    ProjectVersionResultHandler pfHandler = new ProjectVersionResultHandler();

    List<ProjectFileHandler> projectFiles = null;
    try {
      projectFiles =
          runner.query(connection,
              ProjectVersionResultHandler.SELECT_PROJECT_VERSION, pfHandler,
              projectId, version);
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Query for uploaded file for project id " + projectId + " failed.", e);
    }
    if (projectFiles == null || projectFiles.isEmpty()) {
      return null;
    }

    ProjectFileHandler projHandler = projectFiles.get(0);
    int numChunks = projHandler.getNumChunks();
    BufferedOutputStream bStream = null;
    File file = null;
    try {
      try {
        file =
            File.createTempFile(projHandler.getFileName(),
                String.valueOf(version), tempDir);

        bStream = new BufferedOutputStream(new FileOutputStream(file));
      } catch (IOException e) {
        throw new ProjectManagerException(
            "Error creating temp file for stream.");
      }

      int collect = 5;
      int fromChunk = 0;
      int toChunk = collect;
      do {
        ProjectFileChunkResultHandler chunkHandler =
            new ProjectFileChunkResultHandler();
        List<byte[]> data = null;
        try {
          data =
              runner.query(connection,
                  ProjectFileChunkResultHandler.SELECT_PROJECT_CHUNKS_FILE,
                  chunkHandler, projectId, version, fromChunk, toChunk);
        } catch (SQLException e) {
          logger.error(e);
          throw new ProjectManagerException("Query for uploaded file for "
              + projectId + " failed.", e);
        }

        try {
          for (byte[] d : data) {
            bStream.write(d);
          }
        } catch (IOException e) {
          throw new ProjectManagerException("Error writing file", e);
        }

        // Add all the bytes to the stream.
        fromChunk += collect;
        toChunk += collect;
      } while (fromChunk <= numChunks);
    } finally {
      IOUtils.closeQuietly(bStream);
    }

    // Check md5.
    byte[] md5 = null;
    try {
      md5 = Md5Hasher.md5Hash(file);
    } catch (IOException e) {
      throw new ProjectManagerException("Error getting md5 hash.", e);
    }

    if (Arrays.equals(projHandler.getMd5Hash(), md5)) {
      logger.info("Md5 Hash is valid");
    } else {
      throw new ProjectManagerException("Md5 Hash failed on retrieval of file");
    }

    projHandler.setLocalFile(file);
    return projHandler;
  }

  @Override
  public void changeProjectVersion(Project project, int version, String user)
      throws ProjectManagerException {
    long timestamp = System.currentTimeMillis();
    QueryRunner runner = createQueryRunner();
    try {
      final String UPDATE_PROJECT_VERSION =
          "UPDATE projects SET version=?,modified_time=?,last_modified_by=? WHERE id=?";

      runner.update(UPDATE_PROJECT_VERSION, version, timestamp, user,
          project.getId());
      project.setVersion(version);
      project.setLastModifiedTimestamp(timestamp);
      project.setLastModifiedUser(user);
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Error updating switching project version " + project.getName(), e);
    }
  }

  @Override
  public void updatePermission(Project project, String name, Permission perm,
      boolean isGroup) throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();

    if (this.allowsOnDuplicateKey()) {
      long updateTime = System.currentTimeMillis();
      final String INSERT_PROJECT_PERMISSION =
          "INSERT INTO project_permissions (project_id, modified_time, name, permissions, isGroup) values (?,?,?,?,?)"
              + "ON DUPLICATE KEY UPDATE modified_time = VALUES(modified_time), permissions = VALUES(permissions)";

      try {
        runner.update(INSERT_PROJECT_PERMISSION, project.getId(), updateTime,
            name, perm.toFlags(), isGroup);
      } catch (SQLException e) {
        logger.error(e);
        throw new ProjectManagerException("Error updating project "
            + project.getName() + " permissions for " + name, e);
      }
    } else {
      long updateTime = System.currentTimeMillis();
      final String MERGE_PROJECT_PERMISSION =
          "MERGE INTO project_permissions (project_id, modified_time, name, permissions, isGroup) KEY (project_id, name) values (?,?,?,?,?)";

      try {
        runner.update(MERGE_PROJECT_PERMISSION, project.getId(), updateTime,
            name, perm.toFlags(), isGroup);
      } catch (SQLException e) {
        logger.error(e);
        throw new ProjectManagerException("Error updating project "
            + project.getName() + " permissions for " + name, e);
      }
    }

    if (isGroup) {
      project.setGroupPermission(name, perm);
    } else {
      project.setUserPermission(name, perm);
    }
  }

  @Override
  public void updateProjectSettings(Project project)
      throws ProjectManagerException {
    Connection connection = getConnection();
    try {
      updateProjectSettings(connection, project, defaultEncodingType);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException("Error updating project settings", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void updateProjectSettings(Connection connection, Project project,
      EncodingType encType) throws ProjectManagerException {
    QueryRunner runner = new QueryRunner();
    final String UPDATE_PROJECT_SETTINGS =
        "UPDATE projects SET enc_type=?, settings_blob=? WHERE id=?";

    String json = JSONUtils.toJSON(project.toObject());
    byte[] data = null;
    try {
      byte[] stringData = json.getBytes("UTF-8");
      data = stringData;

      if (encType == EncodingType.GZIP) {
        data = GZIPUtils.gzipBytes(stringData);
      }
      logger.debug("NumChars: " + json.length() + " UTF-8:" + stringData.length
          + " Gzip:" + data.length);
    } catch (IOException e) {
      throw new ProjectManagerException("Failed to encode. ", e);
    }

    try {
      runner.update(connection, UPDATE_PROJECT_SETTINGS, encType.getNumVal(),
          data, project.getId());
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException("Error updating project "
          + project.getName() + " version " + project.getVersion(), e);
    }
  }

  @Override
  public void removePermission(Project project, String name, boolean isGroup)
      throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();
    final String DELETE_PROJECT_PERMISSION =
        "DELETE FROM project_permissions WHERE project_id=? AND name=? AND isGroup=?";

    try {
      runner.update(DELETE_PROJECT_PERMISSION, project.getId(), name, isGroup);
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error deleting project "
          + project.getName() + " permissions for " + name, e);
    }

    if (isGroup) {
      project.removeGroupPermission(name);
    } else {
      project.removeUserPermission(name);
    }
  }

  @Override
  public List<Triple<String, Boolean, Permission>> getProjectPermissions(
      int projectId) throws ProjectManagerException {
    ProjectPermissionsResultHandler permHander =
        new ProjectPermissionsResultHandler();
    QueryRunner runner = createQueryRunner();
    List<Triple<String, Boolean, Permission>> permissions = null;
    try {
      permissions =
          runner.query(
              ProjectPermissionsResultHandler.SELECT_PROJECT_PERMISSION,
              permHander, projectId);
    } catch (SQLException e) {
      throw new ProjectManagerException("Query for permissions for "
          + projectId + " failed.", e);
    }

    return permissions;
  }

  @Override
  public void removeProject(Project project, String user)
      throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();

    long updateTime = System.currentTimeMillis();
    final String UPDATE_INACTIVE_PROJECT =
        "UPDATE projects SET active=false,modified_time=?,last_modified_by=? WHERE id=?";
    try {
      runner.update(UPDATE_INACTIVE_PROJECT, updateTime, user, project.getId());
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error marking project "
          + project.getName() + " as inactive", e);
    }
  }

  @Override
  public boolean postEvent(Project project, EventType type, String user,
      String message) {
    QueryRunner runner = createQueryRunner();

    final String INSERT_PROJECT_EVENTS =
        "INSERT INTO project_events (project_id, event_type, event_time, username, message) values (?,?,?,?,?)";
    long updateTime = System.currentTimeMillis();
    try {
      runner.update(INSERT_PROJECT_EVENTS, project.getId(), type.getNumVal(),
          updateTime, user, message);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

  /**
   * Get all the logs for a given project
   *
   * @param project
   * @return
   * @throws ProjectManagerException
   */
  public List<ProjectLogEvent> getProjectEvents(Project project, int num,
      int skip) throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();

    ProjectLogsResultHandler logHandler = new ProjectLogsResultHandler();
    List<ProjectLogEvent> events = null;
    try {
      events =
          runner.query(ProjectLogsResultHandler.SELECT_PROJECT_EVENTS_ORDER,
              logHandler, project.getId(), num, skip);
    } catch (SQLException e) {
      logger.error(e);
    }

    return events;
  }

  @Override
  public void updateDescription(Project project, String description, String user)
      throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();

    final String UPDATE_PROJECT_DESCRIPTION =
        "UPDATE projects SET description=?,modified_time=?,last_modified_by=? WHERE id=?";
    long updateTime = System.currentTimeMillis();
    try {
      runner.update(UPDATE_PROJECT_DESCRIPTION, description, updateTime, user,
          project.getId());
      project.setDescription(description);
      project.setLastModifiedTimestamp(updateTime);
      project.setLastModifiedUser(user);
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error marking project "
          + project.getName() + " as inactive", e);
    }
  }

  @Override
  public int getLatestProjectVersion(Project project)
      throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();

    IntHander handler = new IntHander();
    try {
      return runner.query(IntHander.SELECT_LATEST_VERSION, handler,
          project.getId());
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error marking project "
          + project.getName() + " as inactive", e);
    }
  }

  @Override
  public void uploadFlows(Project project, int version, Collection<Flow> flows)
      throws ProjectManagerException {
    // We do one at a time instead of batch... because well, the batch could be
    // large.
    logger.info("Uploading flows");
    Connection connection = getConnection();

    try {
      for (Flow flow : flows) {
        uploadFlow(connection, project, version, flow, defaultEncodingType);
      }
      connection.commit();
    } catch (IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    } catch (SQLException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void uploadFlow(Project project, int version, Flow flow)
      throws ProjectManagerException {
    logger.info("Uploading flows");
    Connection connection = getConnection();

    try {
      uploadFlow(connection, project, version, flow, defaultEncodingType);
      connection.commit();
    } catch (IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    } catch (SQLException e) {
      throw new ProjectManagerException("Flow Upload failed commit.", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void updateFlow(Project project, int version, Flow flow)
      throws ProjectManagerException {
    logger.info("Uploading flows");
    Connection connection = getConnection();

    try {
      QueryRunner runner = new QueryRunner();
      String json = JSONUtils.toJSON(flow.toObject());
      byte[] stringData = json.getBytes("UTF-8");
      byte[] data = stringData;

      if (defaultEncodingType == EncodingType.GZIP) {
        data = GZIPUtils.gzipBytes(stringData);
      }

      logger.info("Flow upload " + flow.getId() + " is byte size "
          + data.length);
      final String UPDATE_FLOW =
          "UPDATE project_flows SET encoding_type=?,json=? WHERE project_id=? AND version=? AND flow_id=?";
      try {
        runner.update(connection, UPDATE_FLOW, defaultEncodingType.getNumVal(),
            data, project.getId(), version, flow.getId());
      } catch (SQLException e) {
        e.printStackTrace();
        throw new ProjectManagerException("Error inserting flow "
            + flow.getId(), e);
      }
      connection.commit();
    } catch (IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    } catch (SQLException e) {
      throw new ProjectManagerException("Flow Upload failed commit.", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  public EncodingType getDefaultEncodingType() {
    return defaultEncodingType;
  }

  public void setDefaultEncodingType(EncodingType defaultEncodingType) {
    this.defaultEncodingType = defaultEncodingType;
  }

  private void uploadFlow(Connection connection, Project project, int version,
      Flow flow, EncodingType encType) throws ProjectManagerException,
      IOException {
    QueryRunner runner = new QueryRunner();
    String json = JSONUtils.toJSON(flow.toObject());
    byte[] stringData = json.getBytes("UTF-8");
    byte[] data = stringData;

    if (encType == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(stringData);
    }

    logger.info("Flow upload " + flow.getId() + " is byte size " + data.length);
    final String INSERT_FLOW =
        "INSERT INTO project_flows (project_id, version, flow_id, modified_time, encoding_type, json) values (?,?,?,?,?,?)";
    try {
      runner.update(connection, INSERT_FLOW, project.getId(), version,
          flow.getId(), System.currentTimeMillis(), encType.getNumVal(), data);
    } catch (SQLException e) {
      throw new ProjectManagerException("Error inserting flow " + flow.getId(),
          e);
    }
  }

  @Override
  public Flow fetchFlow(Project project, String flowId)
      throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();
    ProjectFlowsResultHandler handler = new ProjectFlowsResultHandler();

    try {
      List<Flow> flows =
          runner.query(ProjectFlowsResultHandler.SELECT_PROJECT_FLOW, handler,
              project.getId(), project.getVersion(), flowId);
      if (flows.isEmpty()) {
        return null;
      } else {
        return flows.get(0);
      }
    } catch (SQLException e) {
      throw new ProjectManagerException("Error fetching flow " + flowId, e);
    }
  }

  @Override
  public List<Flow> fetchAllProjectFlows(Project project)
      throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();
    ProjectFlowsResultHandler handler = new ProjectFlowsResultHandler();

    List<Flow> flows = null;
    try {
      flows =
          runner.query(ProjectFlowsResultHandler.SELECT_ALL_PROJECT_FLOWS,
              handler, project.getId(), project.getVersion());
    } catch (SQLException e) {
      throw new ProjectManagerException("Error fetching flows from project "
          + project.getName() + " version " + project.getVersion(), e);
    }

    return flows;
  }

  @Override
  public void uploadProjectProperties(Project project, List<Props> properties)
      throws ProjectManagerException {
    Connection connection = getConnection();

    try {
      for (Props props : properties) {
        uploadProjectProperty(connection, project, props.getSource(), props);
      }
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException(
          "Error uploading project property files", e);
    } catch (IOException e) {
      throw new ProjectManagerException(
          "Error uploading project property files", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void uploadProjectProperty(Project project, Props props)
      throws ProjectManagerException {
    Connection connection = getConnection();
    try {
      uploadProjectProperty(connection, project, props.getSource(), props);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException(
          "Error uploading project property files", e);
    } catch (IOException e) {
      throw new ProjectManagerException(
          "Error uploading project property file", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void updateProjectProperty(Project project, Props props)
      throws ProjectManagerException {
    Connection connection = getConnection();
    try {
      updateProjectProperty(connection, project, props.getSource(), props);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException(
          "Error uploading project property files", e);
    } catch (IOException e) {
      throw new ProjectManagerException(
          "Error uploading project property file", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void updateProjectProperty(Connection connection, Project project,
      String name, Props props) throws ProjectManagerException, IOException {
    QueryRunner runner = new QueryRunner();
    final String UPDATE_PROPERTIES =
        "UPDATE project_properties SET property=? WHERE project_id=? AND version=? AND name=?";

    String propertyJSON = PropsUtils.toJSONString(props, true);
    byte[] data = propertyJSON.getBytes("UTF-8");
    if (defaultEncodingType == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(data);
    }

    try {
      runner.update(connection, UPDATE_PROPERTIES, data, project.getId(),
          project.getVersion(), name);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException("Error updating property "
          + project.getName() + " version " + project.getVersion(), e);
    }
  }

  private void uploadProjectProperty(Connection connection, Project project,
      String name, Props props) throws ProjectManagerException, IOException {
    QueryRunner runner = new QueryRunner();
    final String INSERT_PROPERTIES =
        "INSERT INTO project_properties (project_id, version, name, modified_time, encoding_type, property) values (?,?,?,?,?,?)";

    String propertyJSON = PropsUtils.toJSONString(props, true);
    byte[] data = propertyJSON.getBytes("UTF-8");
    if (defaultEncodingType == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(data);
    }

    try {
      runner.update(connection, INSERT_PROPERTIES, project.getId(),
          project.getVersion(), name, System.currentTimeMillis(),
          defaultEncodingType.getNumVal(), data);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException("Error uploading project properties "
          + name + " into " + project.getName() + " version "
          + project.getVersion(), e);
    }
  }

  @Override
  public Props fetchProjectProperty(int projectId, int projectVer,
      String propsName) throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();

    ProjectPropertiesResultsHandler handler =
        new ProjectPropertiesResultsHandler();
    try {
      List<Pair<String, Props>> properties =
          runner.query(ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTY,
              handler, projectId, projectVer, propsName);

      if (properties == null || properties.isEmpty()) {
        return null;
      }

      return properties.get(0).getSecond();
    } catch (SQLException e) {
      throw new ProjectManagerException("Error fetching property " + propsName,
          e);
    }
  }

  @Override
  public Props fetchProjectProperty(Project project, String propsName)
      throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();

    ProjectPropertiesResultsHandler handler =
        new ProjectPropertiesResultsHandler();
    try {
      List<Pair<String, Props>> properties =
          runner.query(ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTY,
              handler, project.getId(), project.getVersion(), propsName);

      if (properties == null || properties.isEmpty()) {
        return null;
      }

      return properties.get(0).getSecond();
    } catch (SQLException e) {
      throw new ProjectManagerException("Error fetching property " + propsName,
          e);
    }
  }

  @Override
  public void cleanOlderProjectVersion(int projectId, int version)
      throws ProjectManagerException {
    Connection connection = getConnection();

    try {
      cleanOlderProjectVersionFlows(connection, projectId, version);
      cleanOlderProjectVersionProperties(connection, projectId, version);
      cleanOlderProjectFiles(connection, projectId, version);
      cleanOlderProjectVersion(connection, projectId, version);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void cleanOlderProjectVersionFlows(Connection connection,
      int projectId, int version) throws ProjectManagerException {
    final String DELETE_FLOW =
        "DELETE FROM project_flows WHERE project_id=? AND version<?";
    QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, DELETE_FLOW, projectId, version);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException("Error deleting project version flows "
          + projectId + ":" + version, e);
    }
  }

  private void cleanOlderProjectVersionProperties(Connection connection,
      int projectId, int version) throws ProjectManagerException {
    final String DELETE_PROPERTIES =
        "DELETE FROM project_properties WHERE project_id=? AND version<?";
    QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, DELETE_PROPERTIES, projectId, version);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException(
          "Error deleting project version properties " + projectId + ":"
              + version, e);
    }
  }

  private void cleanOlderProjectFiles(Connection connection, int projectId,
      int version) throws ProjectManagerException {
    final String DELETE_PROJECT_FILES =
        "DELETE FROM project_files WHERE project_id=? AND version<?";
    QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, DELETE_PROJECT_FILES, projectId, version);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException("Error deleting project version files "
          + projectId + ":" + version, e);
    }
  }

  private void cleanOlderProjectVersion(Connection connection, int projectId,
      int version) throws ProjectManagerException {
    final String UPDATE_PROJECT_VERSIONS =
        "UPDATE project_versions SET num_chunks=0 WHERE project_id=? AND version<?";
    QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, UPDATE_PROJECT_VERSIONS, projectId, version);
      connection.commit();
    } catch (SQLException e) {
      throw new ProjectManagerException(
          "Error updating project version chunksize " + projectId + ":"
              + version, e);
    }
  }

  @Override
  public Map<String, Props> fetchProjectProperties(int projectId, int version)
      throws ProjectManagerException {
    QueryRunner runner = createQueryRunner();

    ProjectPropertiesResultsHandler handler =
        new ProjectPropertiesResultsHandler();
    try {
      List<Pair<String, Props>> properties =
          runner.query(
              ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTIES,
              handler, projectId, version);

      if (properties == null || properties.isEmpty()) {
        return null;
      }

      HashMap<String, Props> props = new HashMap<String, Props>();
      for (Pair<String, Props> pair : properties) {
        props.put(pair.getFirst(), pair.getSecond());
      }
      return props;
    } catch (SQLException e) {
      throw new ProjectManagerException("Error fetching properties", e);
    }
  }

  private static class ProjectResultHandler implements
      ResultSetHandler<List<Project>> {
    private static String SELECT_PROJECT_BY_NAME =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE name=?";

    private static String SELECT_PROJECT_BY_ID =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE id=?";

    private static String SELECT_ALL_ACTIVE_PROJECTS =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE active=true";

    private static String SELECT_ACTIVE_PROJECT_BY_NAME =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE name=? AND active=true";

    @Override
    public List<Project> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Project> emptyList();
      }

      ArrayList<Project> projects = new ArrayList<Project>();
      do {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        boolean active = rs.getBoolean(3);
        long modifiedTime = rs.getLong(4);
        long createTime = rs.getLong(5);
        int version = rs.getInt(6);
        String lastModifiedBy = rs.getString(7);
        String description = rs.getString(8);
        int encodingType = rs.getInt(9);
        byte[] data = rs.getBytes(10);

        Project project;
        if (data != null) {
          EncodingType encType = EncodingType.fromInteger(encodingType);
          Object blobObj;
          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              blobObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              String jsonString = new String(data, "UTF-8");
              blobObj = JSONUtils.parseJSONFromString(jsonString);
            }
            project = Project.projectFromObject(blobObj);
          } catch (IOException e) {
            throw new SQLException("Failed to get project.", e);
          }
        } else {
          project = new Project(id, name);
        }

        // update the fields as they may have changed

        project.setActive(active);
        project.setLastModifiedTimestamp(modifiedTime);
        project.setCreateTimestamp(createTime);
        project.setVersion(version);
        project.setLastModifiedUser(lastModifiedBy);
        project.setDescription(description);

        projects.add(project);
      } while (rs.next());

      return projects;
    }
  }

  private static class ProjectPermissionsResultHandler implements
      ResultSetHandler<List<Triple<String, Boolean, Permission>>> {
    private static String SELECT_PROJECT_PERMISSION =
        "SELECT project_id, modified_time, name, permissions, isGroup FROM project_permissions WHERE project_id=?";

    @Override
    public List<Triple<String, Boolean, Permission>> handle(ResultSet rs)
        throws SQLException {
      if (!rs.next()) {
        return Collections.<Triple<String, Boolean, Permission>> emptyList();
      }

      ArrayList<Triple<String, Boolean, Permission>> permissions =
          new ArrayList<Triple<String, Boolean, Permission>>();
      do {
        String username = rs.getString(3);
        int permissionFlag = rs.getInt(4);
        boolean val = rs.getBoolean(5);

        Permission perm = new Permission(permissionFlag);
        permissions.add(new Triple<String, Boolean, Permission>(username, val,
            perm));
      } while (rs.next());

      return permissions;
    }
  }

  private static class ProjectFlowsResultHandler implements
      ResultSetHandler<List<Flow>> {
    private static String SELECT_PROJECT_FLOW =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json FROM project_flows WHERE project_id=? AND version=? AND flow_id=?";

    private static String SELECT_ALL_PROJECT_FLOWS =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json FROM project_flows WHERE project_id=? AND version=?";

    @Override
    public List<Flow> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Flow> emptyList();
      }

      ArrayList<Flow> flows = new ArrayList<Flow>();
      do {
        String flowId = rs.getString(3);
        int encodingType = rs.getInt(5);
        byte[] dataBytes = rs.getBytes(6);

        if (dataBytes == null) {
          continue;
        }

        EncodingType encType = EncodingType.fromInteger(encodingType);

        Object flowObj = null;
        try {
          // Convoluted way to inflate strings. Should find common package or
          // helper function.
          if (encType == EncodingType.GZIP) {
            // Decompress the sucker.
            String jsonString = GZIPUtils.unGzipString(dataBytes, "UTF-8");
            flowObj = JSONUtils.parseJSONFromString(jsonString);
          } else {
            String jsonString = new String(dataBytes, "UTF-8");
            flowObj = JSONUtils.parseJSONFromString(jsonString);
          }

          Flow flow = Flow.flowFromObject(flowObj);
          flows.add(flow);
        } catch (IOException e) {
          throw new SQLException("Error retrieving flow data " + flowId, e);
        }

      } while (rs.next());

      return flows;
    }
  }

  private static class ProjectPropertiesResultsHandler implements
      ResultSetHandler<List<Pair<String, Props>>> {
    private static String SELECT_PROJECT_PROPERTY =
        "SELECT project_id, version, name, modified_time, encoding_type, property FROM project_properties WHERE project_id=? AND version=? AND name=?";

    private static String SELECT_PROJECT_PROPERTIES =
        "SELECT project_id, version, name, modified_time, encoding_type, property FROM project_properties WHERE project_id=? AND version=?";

    @Override
    public List<Pair<String, Props>> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Pair<String, Props>> emptyList();
      }

      List<Pair<String, Props>> properties =
          new ArrayList<Pair<String, Props>>();
      do {
        String name = rs.getString(3);
        int eventType = rs.getInt(5);
        byte[] dataBytes = rs.getBytes(6);

        EncodingType encType = EncodingType.fromInteger(eventType);
        String propertyString = null;

        try {
          if (encType == EncodingType.GZIP) {
            // Decompress the sucker.
            propertyString = GZIPUtils.unGzipString(dataBytes, "UTF-8");
          } else {
            propertyString = new String(dataBytes, "UTF-8");
          }

          Props props = PropsUtils.fromJSONString(propertyString);
          props.setSource(name);
          properties.add(new Pair<String, Props>(name, props));
        } catch (IOException e) {
          throw new SQLException(e);
        }
      } while (rs.next());

      return properties;
    }
  }

  private static class ProjectLogsResultHandler implements
      ResultSetHandler<List<ProjectLogEvent>> {
    private static String SELECT_PROJECT_EVENTS_ORDER =
        "SELECT project_id, event_type, event_time, username, message FROM project_events WHERE project_id=? ORDER BY event_time DESC LIMIT ? OFFSET ?";

    @Override
    public List<ProjectLogEvent> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ProjectLogEvent> emptyList();
      }

      ArrayList<ProjectLogEvent> events = new ArrayList<ProjectLogEvent>();
      do {
        int projectId = rs.getInt(1);
        int eventType = rs.getInt(2);
        long eventTime = rs.getLong(3);
        String username = rs.getString(4);
        String message = rs.getString(5);

        ProjectLogEvent event =
            new ProjectLogEvent(projectId, EventType.fromInteger(eventType),
                eventTime, username, message);
        events.add(event);
      } while (rs.next());

      return events;
    }
  }

  private static class ProjectFileChunkResultHandler implements
      ResultSetHandler<List<byte[]>> {
    private static String SELECT_PROJECT_CHUNKS_FILE =
        "SELECT project_id, version, chunk, size, file FROM project_files WHERE project_id=? AND version=? AND chunk >= ? AND chunk < ? ORDER BY chunk ASC";

    @Override
    public List<byte[]> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<byte[]> emptyList();
      }

      ArrayList<byte[]> data = new ArrayList<byte[]>();
      do {
        byte[] bytes = rs.getBytes(5);

        data.add(bytes);
      } while (rs.next());

      return data;
    }

  }

  private static class ProjectVersionResultHandler implements
      ResultSetHandler<List<ProjectFileHandler>> {
    private static String SELECT_PROJECT_VERSION =
        "SELECT project_id, version, upload_time, uploader, file_type, file_name, md5, num_chunks FROM project_versions WHERE project_id=? AND version=?";

    @Override
    public List<ProjectFileHandler> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }

      List<ProjectFileHandler> handlers = new ArrayList<ProjectFileHandler>();
      do {
        int projectId = rs.getInt(1);
        int version = rs.getInt(2);
        long uploadTime = rs.getLong(3);
        String uploader = rs.getString(4);
        String fileType = rs.getString(5);
        String fileName = rs.getString(6);
        byte[] md5 = rs.getBytes(7);
        int numChunks = rs.getInt(8);

        ProjectFileHandler handler =
            new ProjectFileHandler(projectId, version, uploadTime, uploader,
                fileType, fileName, numChunks, md5);

        handlers.add(handler);
      } while (rs.next());

      return handlers;
    }
  }

  private static class IntHander implements ResultSetHandler<Integer> {
    private static String SELECT_LATEST_VERSION =
        "SELECT MAX(version) FROM project_versions WHERE project_id=?";

    @Override
    public Integer handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return 0;
      }

      return rs.getInt(1);
    }
  }

  private Connection getConnection() throws ProjectManagerException {
    Connection connection = null;
    try {
      connection = super.getDBConnection(false);
    } catch (Exception e) {
      DbUtils.closeQuietly(connection);
      throw new ProjectManagerException("Error getting DB connection.", e);
    }

    return connection;
  }
}
