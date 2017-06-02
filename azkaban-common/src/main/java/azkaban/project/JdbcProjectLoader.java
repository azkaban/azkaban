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
import com.google.common.io.Files;
import com.google.inject.Inject;
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

public class JdbcProjectLoader extends AbstractJdbcLoader implements
    ProjectLoader {

  private static final Logger logger = Logger
      .getLogger(JdbcProjectLoader.class);

  private static final int CHUCK_SIZE = 1024 * 1024 * 10;
  private final File tempDir;

  private EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  public JdbcProjectLoader(final Props props) {
    super(props);
    this.tempDir = new File(props.getString("project.temp.dir", "temp"));
    if (!this.tempDir.exists()) {
      this.tempDir.mkdirs();
    }
  }

  @Override
  public List<Project> fetchAllActiveProjects() throws ProjectManagerException {
    final Connection connection = getConnection();

    List<Project> projects = null;
    try {
      projects = fetchAllActiveProjects(connection);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return projects;
  }

  private List<Project> fetchAllActiveProjects(final Connection connection)
      throws ProjectManagerException {
    final QueryRunner runner = new QueryRunner();

    final ProjectResultHandler handler = new ProjectResultHandler();
    List<Project> projects = null;
    try {
      projects =
          runner.query(connection,
              ProjectResultHandler.SELECT_ALL_ACTIVE_PROJECTS, handler);

      for (final Project project : projects) {
        final List<Triple<String, Boolean, Permission>> permissions =
            fetchPermissionsForProject(connection, project);

        for (final Triple<String, Boolean, Permission> entry : permissions) {
          if (entry.getSecond()) {
            project.setGroupPermission(entry.getFirst(), entry.getThird());
          } else {
            project.setUserPermission(entry.getFirst(), entry.getThird());
          }
        }
      }
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error retrieving all projects", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return projects;
  }

  @Override
  public Project fetchProjectById(final int id) throws ProjectManagerException {
    final Connection connection = getConnection();

    Project project = null;
    try {
      project = fetchProjectById(connection, id);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return project;
  }

  private Project fetchProjectById(final Connection connection, final int id)
      throws ProjectManagerException {
    final QueryRunner runner = new QueryRunner();
    // Fetch the project
    Project project = null;
    final ProjectResultHandler handler = new ProjectResultHandler();
    try {
      final List<Project> projects =
          runner.query(connection, ProjectResultHandler.SELECT_PROJECT_BY_ID,
              handler, id);
      if (projects.isEmpty()) {
        throw new ProjectManagerException("No project with id " + id
            + " exists in db.");
      }

      project = projects.get(0);
    } catch (final SQLException e) {
      logger.error(ProjectResultHandler.SELECT_PROJECT_BY_ID + " failed.");
      throw new ProjectManagerException(
          "Query for existing project failed. Project " + id, e);
    }

    // Fetch the user permissions
    final List<Triple<String, Boolean, Permission>> permissions =
        fetchPermissionsForProject(connection, project);

    for (final Triple<String, Boolean, Permission> perm : permissions) {
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
  public Project fetchProjectByName(final String name)
      throws ProjectManagerException {
    final Connection connection = getConnection();

    Project project = null;
    try {
      project = fetchProjectByName(connection, name);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return project;
  }

  private Project fetchProjectByName(final Connection connection, final String name)
      throws ProjectManagerException {
    final QueryRunner runner = new QueryRunner();
    // Fetch the project
    Project project = null;
    final ProjectResultHandler handler = new ProjectResultHandler();
    try {
      final List<Project> projects =
          runner.query(connection,
              ProjectResultHandler.SELECT_PROJECT_BY_NAME, handler, name);
      if (projects.isEmpty()) {
        throw new ProjectManagerException(
            "No project with name " + name + " exists in db.");
      }

      project = projects.get(0);
    } catch (final SQLException e) {
      logger.error(ProjectResultHandler.SELECT_PROJECT_BY_NAME
          + " failed.");
      throw new ProjectManagerException(
          "Query for existing project failed. Project " + name, e);
    }

    // Fetch the user permissions
    final List<Triple<String, Boolean, Permission>> permissions =
        fetchPermissionsForProject(connection, project);

    for (final Triple<String, Boolean, Permission> perm : permissions) {
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
      final Connection connection, final Project project) throws ProjectManagerException {
    final ProjectPermissionsResultHandler permHander =
        new ProjectPermissionsResultHandler();

    final QueryRunner runner = new QueryRunner();
    List<Triple<String, Boolean, Permission>> permissions = null;
    try {
      permissions =
          runner.query(connection,
              ProjectPermissionsResultHandler.SELECT_PROJECT_PERMISSION,
              permHander, project.getId());
    } catch (final SQLException e) {
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
  public Project createNewProject(final String name, final String description, final User creator)
      throws ProjectManagerException {
    final Connection connection = getConnection();

    Project project;
    try {
      // No need to commit, since createNewProject should commit.
      project = createNewProject(connection, name, description, creator);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return project;
  }

  private synchronized Project createNewProject(final Connection connection,
      final String name, final String description, final User creator)
      throws ProjectManagerException {
    final QueryRunner runner = new QueryRunner();
    final ProjectResultHandler handler = new ProjectResultHandler();

    // See if it exists first.
    try {
      final List<Project> project =
          runner
              .query(connection,
                  ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME, handler,
                  name);
      if (!project.isEmpty()) {
        throw new ProjectManagerException("Active project with name " + name
            + " already exists in db.");
      }
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Checking for existing project failed. " + name, e);
    }

    final String INSERT_PROJECT =
        "INSERT INTO projects ( name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob) values (?,?,?,?,?,?,?,?,?)";
    // Insert project
    try {
      final long time = System.currentTimeMillis();
      final int i =
          runner.update(connection, INSERT_PROJECT, name, true, time, time,
              null, creator.getUserId(), description,
              this.defaultEncodingType.getNumVal(), null);
      if (i == 0) {
        throw new ProjectManagerException("No projects have been inserted.");
      }
      connection.commit();

    } catch (final SQLException e) {
      logger.error(INSERT_PROJECT + " failed.");
      try {
        connection.rollback();
      } catch (final SQLException e1) {
        e1.printStackTrace();
      }
      throw new ProjectManagerException(
          "Insert project for existing project failed. " + name, e);
    }

    // Do another query to grab and return the project.
    Project project = null;
    try {
      final List<Project> projects =
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
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Checking for existing project failed. " + name, e);
    }

    return project;
  }

  @Override
  public void uploadProjectFile(final int projectId, final int version, final File localFile,
      final String uploader)
      throws ProjectManagerException {
    final long startMs = System.currentTimeMillis();
    logger.info(String.format("Uploading Project ID: %d file: %s [%d bytes]",
        projectId, localFile.getName(), localFile.length()));
    final Connection connection = getConnection();

    try {
      /* Update DB with new project info */
      addProjectToProjectVersions(
          connection, projectId, version, localFile, uploader, computeHash(localFile), null);

      uploadProjectFile(connection, projectId, version, localFile);

      connection.commit();
      final long duration = (System.currentTimeMillis() - startMs) / 1000;
      logger.info(String.format("Uploaded Project ID: %d file: %s [%d bytes] in %d sec",
          projectId, localFile.getName(), localFile.length(), duration));
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error getting DB connection.", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void uploadProjectFile(final Connection connection, final int projectId,
      final int version, final File localFile)
      throws ProjectManagerException {
    /* Step 1: Upload File in chunks to DB */
    final int chunks = uploadFileInChunks(connection, projectId, version, localFile);

    /* Step 2: Update number of chunks in DB */
    updateChunksInProjectVersions(connection, projectId, version, chunks);
  }

  @Override
  public void addProjectVersion(
      final int projectId,
      final int version,
      final File localFile,
      final String uploader,
      final byte[] md5,
      final String resourceId) throws ProjectManagerException {
    try (Connection connection = getConnection()) {
      addProjectToProjectVersions(connection, projectId, version, localFile, uploader, md5,
          resourceId);
      connection.commit();
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          String.format("Add ProjectVersion failed. project id: %d version: %d",
              projectId, version), e);
    }
  }

  /**
   * Insert a new version record to TABLE project_versions before uploading files.
   *
   * The reason for this operation:
   * When error chunking happens in remote mysql server, incomplete file data remains
   * in DB, and an SQL exception is thrown. If we don't have this operation before uploading file,
   * the SQL exception prevents AZ from creating the new version record in Table project_versions.
   * However, the Table project_files still reserve the incomplete files, which causes troubles
   * when uploading a new file: Since the version in TABLE project_versions is still old, mysql will stop
   * inserting new files to db.
   *
   * Why this operation is safe:
   * When AZ uploads a new zip file, it always fetches the latest version proj_v from TABLE project_version,
   * proj_v+1 will be used as the new version for the uploading files.
   *
   * Assume error chunking happens on day 1. proj_v is created for this bad file (old file version + 1).
   * When we upload a new project zip in day2, new file in day 2 will use the new version (proj_v + 1).
   * When file uploading completes, AZ will clean all old chunks in DB afterward.
   */
  private void addProjectToProjectVersions(final Connection connection,
      final int projectId,
      final int version,
      final File localFile,
      final String uploader,
      final byte[] md5,
      final String resourceId) throws ProjectManagerException {
    final long updateTime = System.currentTimeMillis();
    final QueryRunner runner = new QueryRunner();

    final String INSERT_PROJECT_VERSION = "INSERT INTO project_versions "
        + "(project_id, version, upload_time, uploader, file_type, file_name, md5, num_chunks, resource_id) values "
        + "(?,?,?,?,?,?,?,?,?)";

    try {
      /*
       * As we don't know the num_chunks before uploading the file, we initialize it to 0,
       * and will update it after uploading completes.
       */
      runner.update(connection,
          INSERT_PROJECT_VERSION,
          projectId,
          version,
          updateTime,
          uploader,
          Files.getFileExtension(localFile.getName()),
          localFile.getName(),
          md5,
          0,
          resourceId);
    } catch (final SQLException e) {
      final String msg = String
          .format("Error initializing project id: %d version: %d ", projectId, version);
      logger.error(msg, e);
      throw new ProjectManagerException(msg, e);
    }
  }

  private byte[] computeHash(final File localFile) {
    logger.info("Creating message digest for upload " + localFile.getName());
    final byte[] md5;
    try {
      md5 = Md5Hasher.md5Hash(localFile);
    } catch (final IOException e) {
      throw new ProjectManagerException("Error getting md5 hash.", e);
    }

    logger.info("Md5 hash created");
    return md5;
  }

  private int uploadFileInChunks(final Connection connection, final int projectId,
      final int version, final File localFile)
      throws ProjectManagerException {
    final QueryRunner runner = new QueryRunner();

    // Really... I doubt we'll get a > 2gig file. So int casting it is!
    final byte[] buffer = new byte[CHUCK_SIZE];
    final String INSERT_PROJECT_FILES =
        "INSERT INTO project_files (project_id, version, chunk, size, file) values (?,?,?,?,?)";

    BufferedInputStream bufferedStream = null;
    int chunk = 0;
    try {
      bufferedStream = new BufferedInputStream(new FileInputStream(localFile));
      int size = bufferedStream.read(buffer);
      while (size >= 0) {
        logger.info("Read bytes for " + localFile.getName() + " size:" + size);
        byte[] buf = buffer;
        if (size < buffer.length) {
          buf = Arrays.copyOfRange(buffer, 0, size);
        }
        try {
          logger.info("Running update for " + localFile.getName() + " chunk " + chunk);
          runner.update(connection, INSERT_PROJECT_FILES, projectId, version, chunk, size, buf);

          /**
           * We enforce az committing to db when uploading every single chunk,
           * in order to reduce the transaction duration and conserve sql server resources.
           *
           * If the files to be uploaded is very large and we don't commit every single chunk,
           * the remote mysql server will run into memory troubles.
           */
          connection.commit();
          logger.info("Finished update for " + localFile.getName() + " chunk " + chunk);
        } catch (final SQLException e) {
          throw new ProjectManagerException("Error Chunking during uploading files to db...");
        }
        ++chunk;

        size = bufferedStream.read(buffer);
      }
    } catch (final IOException e) {
      throw new ProjectManagerException(String.format(
          "Error chunking file. projectId: %d, version: %d, file:%s[%d bytes], chunk: %d",
          projectId, version, localFile.getName(), localFile.length(), chunk));
    } finally {
      IOUtils.closeQuietly(bufferedStream);
    }
    return chunk;
  }

  /**
   * we update num_chunks's actual number to db here.
   */
  private void updateChunksInProjectVersions(final Connection connection, final int projectId,
      final int version,
      final int chunk)
      throws ProjectManagerException {

    final String UPDATE_PROJECT_NUM_CHUNKS =
        "UPDATE project_versions SET num_chunks=? WHERE project_id=? AND version=?";

    final QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, UPDATE_PROJECT_NUM_CHUNKS, chunk, projectId, version);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error updating project " + projectId + " : chunk_num " + chunk, e);
    }
  }

  @Override
  public ProjectFileHandler getUploadedFile(final int projectId, final int version)
      throws ProjectManagerException {
    logger.info("Retrieving to " + projectId + " version:" + version);
    final Connection connection = getConnection();
    ProjectFileHandler handler = null;
    try {
      handler = getUploadedFile(connection, projectId, version);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    return handler;
  }

  @Override
  public ProjectFileHandler fetchProjectMetaData(final int projectId, final int version) {
    final ProjectVersionResultHandler pfHandler = new ProjectVersionResultHandler();

    try (Connection connection = getConnection()) {
      final List<ProjectFileHandler> projectFiles = new QueryRunner().query(connection,
          ProjectVersionResultHandler.SELECT_PROJECT_VERSION, pfHandler, projectId, version);
      if (projectFiles == null || projectFiles.isEmpty()) {
        return null;
      }
      return projectFiles.get(0);
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Query for uploaded file for project id " + projectId + " failed.", e);
    }
  }

  private ProjectFileHandler getUploadedFile(final Connection connection,
      final int projectId, final int version) throws ProjectManagerException {
    final ProjectFileHandler projHandler = fetchProjectMetaData(projectId, version);
    if (projHandler == null) {
      return null;
    }
    final int numChunks = projHandler.getNumChunks();
    BufferedOutputStream bStream = null;
    File file;
    try {
      try {
        file = File
            .createTempFile(projHandler.getFileName(), String.valueOf(version), this.tempDir);
        bStream = new BufferedOutputStream(new FileOutputStream(file));
      } catch (final IOException e) {
        throw new ProjectManagerException(
            "Error creating temp file for stream.");
      }

      final QueryRunner runner = new QueryRunner();
      final int collect = 5;
      int fromChunk = 0;
      int toChunk = collect;
      do {
        final ProjectFileChunkResultHandler chunkHandler =
            new ProjectFileChunkResultHandler();
        List<byte[]> data = null;
        try {
          data =
              runner.query(connection,
                  ProjectFileChunkResultHandler.SELECT_PROJECT_CHUNKS_FILE,
                  chunkHandler, projectId, version, fromChunk, toChunk);
        } catch (final SQLException e) {
          logger.error(e);
          throw new ProjectManagerException("Query for uploaded file for "
              + projectId + " failed.", e);
        }

        try {
          for (final byte[] d : data) {
            bStream.write(d);
          }
        } catch (final IOException e) {
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
    } catch (final IOException e) {
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
  public void changeProjectVersion(final Project project, final int version, final String user)
      throws ProjectManagerException {
    final long timestamp = System.currentTimeMillis();
    final QueryRunner runner = createQueryRunner();
    try {
      final String UPDATE_PROJECT_VERSION =
          "UPDATE projects SET version=?,modified_time=?,last_modified_by=? WHERE id=?";

      runner.update(UPDATE_PROJECT_VERSION, version, timestamp, user,
          project.getId());
      project.setVersion(version);
      project.setLastModifiedTimestamp(timestamp);
      project.setLastModifiedUser(user);
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Error updating switching project version " + project.getName(), e);
    }
  }

  @Override
  public void updatePermission(final Project project, final String name, final Permission perm,
      final boolean isGroup) throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();

    if (this.allowsOnDuplicateKey()) {
      final long updateTime = System.currentTimeMillis();
      final String INSERT_PROJECT_PERMISSION =
          "INSERT INTO project_permissions (project_id, modified_time, name, permissions, isGroup) values (?,?,?,?,?)"
              + "ON DUPLICATE KEY UPDATE modified_time = VALUES(modified_time), permissions = VALUES(permissions)";

      try {
        runner.update(INSERT_PROJECT_PERMISSION, project.getId(), updateTime,
            name, perm.toFlags(), isGroup);
      } catch (final SQLException e) {
        logger.error(e);
        throw new ProjectManagerException("Error updating project "
            + project.getName() + " permissions for " + name, e);
      }
    } else {
      final long updateTime = System.currentTimeMillis();
      final String MERGE_PROJECT_PERMISSION =
          "MERGE INTO project_permissions (project_id, modified_time, name, permissions, isGroup) KEY (project_id, name) values (?,?,?,?,?)";

      try {
        runner.update(MERGE_PROJECT_PERMISSION, project.getId(), updateTime,
            name, perm.toFlags(), isGroup);
      } catch (final SQLException e) {
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
  public void updateProjectSettings(final Project project)
      throws ProjectManagerException {
    final Connection connection = getConnection();
    try {
      updateProjectSettings(connection, project, this.defaultEncodingType);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error updating project settings", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void updateProjectSettings(final Connection connection, final Project project,
      final EncodingType encType) throws ProjectManagerException {
    final QueryRunner runner = new QueryRunner();
    final String UPDATE_PROJECT_SETTINGS =
        "UPDATE projects SET enc_type=?, settings_blob=? WHERE id=?";

    final String json = JSONUtils.toJSON(project.toObject());
    byte[] data = null;
    try {
      final byte[] stringData = json.getBytes("UTF-8");
      data = stringData;

      if (encType == EncodingType.GZIP) {
        data = GZIPUtils.gzipBytes(stringData);
      }
      logger.debug("NumChars: " + json.length() + " UTF-8:" + stringData.length
          + " Gzip:" + data.length);
    } catch (final IOException e) {
      throw new ProjectManagerException("Failed to encode. ", e);
    }

    try {
      runner.update(connection, UPDATE_PROJECT_SETTINGS, encType.getNumVal(),
          data, project.getId());
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error updating project "
          + project.getName() + " version " + project.getVersion(), e);
    }
  }

  @Override
  public void removePermission(final Project project, final String name, final boolean isGroup)
      throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();
    final String DELETE_PROJECT_PERMISSION =
        "DELETE FROM project_permissions WHERE project_id=? AND name=? AND isGroup=?";

    try {
      runner.update(DELETE_PROJECT_PERMISSION, project.getId(), name, isGroup);
    } catch (final SQLException e) {
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
      final int projectId) throws ProjectManagerException {
    final ProjectPermissionsResultHandler permHander =
        new ProjectPermissionsResultHandler();
    final QueryRunner runner = createQueryRunner();
    List<Triple<String, Boolean, Permission>> permissions = null;
    try {
      permissions =
          runner.query(
              ProjectPermissionsResultHandler.SELECT_PROJECT_PERMISSION,
              permHander, projectId);
    } catch (final SQLException e) {
      throw new ProjectManagerException("Query for permissions for "
          + projectId + " failed.", e);
    }

    return permissions;
  }

  @Override
  public void removeProject(final Project project, final String user)
      throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();

    final long updateTime = System.currentTimeMillis();
    final String UPDATE_INACTIVE_PROJECT =
        "UPDATE projects SET active=false,modified_time=?,last_modified_by=? WHERE id=?";
    try {
      runner.update(UPDATE_INACTIVE_PROJECT, updateTime, user, project.getId());
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error marking project "
          + project.getName() + " as inactive", e);
    }
  }

  @Override
  public boolean postEvent(final Project project, final EventType type, final String user,
      final String message) {
    final QueryRunner runner = createQueryRunner();

    final String INSERT_PROJECT_EVENTS =
        "INSERT INTO project_events (project_id, event_type, event_time, username, message) values (?,?,?,?,?)";
    final long updateTime = System.currentTimeMillis();
    try {
      runner.update(INSERT_PROJECT_EVENTS, project.getId(), type.getNumVal(),
          updateTime, user, message);
    } catch (final SQLException e) {
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
  @Override
  public List<ProjectLogEvent> getProjectEvents(final Project project, final int num,
      final int skip) throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();

    final ProjectLogsResultHandler logHandler = new ProjectLogsResultHandler();
    List<ProjectLogEvent> events = null;
    try {
      events =
          runner.query(ProjectLogsResultHandler.SELECT_PROJECT_EVENTS_ORDER,
              logHandler, project.getId(), num, skip);
    } catch (final SQLException e) {
      logger.error(e);
    }

    return events;
  }

  @Override
  public void updateDescription(final Project project, final String description, final String user)
      throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();

    final String UPDATE_PROJECT_DESCRIPTION =
        "UPDATE projects SET description=?,modified_time=?,last_modified_by=? WHERE id=?";
    final long updateTime = System.currentTimeMillis();
    try {
      runner.update(UPDATE_PROJECT_DESCRIPTION, description, updateTime, user,
          project.getId());
      project.setDescription(description);
      project.setLastModifiedTimestamp(updateTime);
      project.setLastModifiedUser(user);
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error marking project "
          + project.getName() + " as inactive", e);
    }
  }

  @Override
  public int getLatestProjectVersion(final Project project)
      throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();

    final IntHander handler = new IntHander();
    try {
      return runner.query(IntHander.SELECT_LATEST_VERSION, handler,
          project.getId());
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error marking project "
          + project.getName() + " as inactive", e);
    }
  }

  @Override
  public void uploadFlows(final Project project, final int version, final Collection<Flow> flows)
      throws ProjectManagerException {
    // We do one at a time instead of batch... because well, the batch could be
    // large.
    logger.info("Uploading flows");
    final Connection connection = getConnection();

    try {
      for (final Flow flow : flows) {
        uploadFlow(connection, project, version, flow, this.defaultEncodingType);
      }
      connection.commit();
    } catch (final IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    } catch (final SQLException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void uploadFlow(final Project project, final int version, final Flow flow)
      throws ProjectManagerException {
    logger.info("Uploading flows");
    final Connection connection = getConnection();

    try {
      uploadFlow(connection, project, version, flow, this.defaultEncodingType);
      connection.commit();
    } catch (final IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    } catch (final SQLException e) {
      throw new ProjectManagerException("Flow Upload failed commit.", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void updateFlow(final Project project, final int version, final Flow flow)
      throws ProjectManagerException {
    logger.info("Uploading flows");
    final Connection connection = getConnection();

    try {
      final QueryRunner runner = new QueryRunner();
      final String json = JSONUtils.toJSON(flow.toObject());
      final byte[] stringData = json.getBytes("UTF-8");
      byte[] data = stringData;

      if (this.defaultEncodingType == EncodingType.GZIP) {
        data = GZIPUtils.gzipBytes(stringData);
      }

      logger.info("Flow upload " + flow.getId() + " is byte size "
          + data.length);
      final String UPDATE_FLOW =
          "UPDATE project_flows SET encoding_type=?,json=? WHERE project_id=? AND version=? AND flow_id=?";
      try {
        runner.update(connection, UPDATE_FLOW, this.defaultEncodingType.getNumVal(),
            data, project.getId(), version, flow.getId());
      } catch (final SQLException e) {
        e.printStackTrace();
        throw new ProjectManagerException("Error inserting flow "
            + flow.getId(), e);
      }
      connection.commit();
    } catch (final IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    } catch (final SQLException e) {
      throw new ProjectManagerException("Flow Upload failed commit.", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  public EncodingType getDefaultEncodingType() {
    return this.defaultEncodingType;
  }

  public void setDefaultEncodingType(final EncodingType defaultEncodingType) {
    this.defaultEncodingType = defaultEncodingType;
  }

  private void uploadFlow(final Connection connection, final Project project, final int version,
      final Flow flow, final EncodingType encType) throws ProjectManagerException,
      IOException {
    final QueryRunner runner = new QueryRunner();
    final String json = JSONUtils.toJSON(flow.toObject());
    final byte[] stringData = json.getBytes("UTF-8");
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
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error inserting flow " + flow.getId(),
          e);
    }
  }

  @Override
  public Flow fetchFlow(final Project project, final String flowId)
      throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();
    final ProjectFlowsResultHandler handler = new ProjectFlowsResultHandler();

    try {
      final List<Flow> flows =
          runner.query(ProjectFlowsResultHandler.SELECT_PROJECT_FLOW, handler,
              project.getId(), project.getVersion(), flowId);
      if (flows.isEmpty()) {
        return null;
      } else {
        return flows.get(0);
      }
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error fetching flow " + flowId, e);
    }
  }

  @Override
  public List<Flow> fetchAllProjectFlows(final Project project)
      throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();
    final ProjectFlowsResultHandler handler = new ProjectFlowsResultHandler();

    List<Flow> flows = null;
    try {
      flows =
          runner.query(ProjectFlowsResultHandler.SELECT_ALL_PROJECT_FLOWS,
              handler, project.getId(), project.getVersion());
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error fetching flows from project "
          + project.getName() + " version " + project.getVersion(), e);
    }

    return flows;
  }

  @Override
  public void uploadProjectProperties(final Project project, final List<Props> properties)
      throws ProjectManagerException {
    final Connection connection = getConnection();

    try {
      for (final Props props : properties) {
        uploadProjectProperty(connection, project, props.getSource(), props);
      }
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error uploading project property files", e);
    } catch (final IOException e) {
      throw new ProjectManagerException(
          "Error uploading project property files", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void uploadProjectProperty(final Project project, final Props props)
      throws ProjectManagerException {
    final Connection connection = getConnection();
    try {
      uploadProjectProperty(connection, project, props.getSource(), props);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error uploading project property files", e);
    } catch (final IOException e) {
      throw new ProjectManagerException(
          "Error uploading project property file", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void updateProjectProperty(final Project project, final Props props)
      throws ProjectManagerException {
    final Connection connection = getConnection();
    try {
      updateProjectProperty(connection, project, props.getSource(), props);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error uploading project property files", e);
    } catch (final IOException e) {
      throw new ProjectManagerException(
          "Error uploading project property file", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void updateProjectProperty(final Connection connection, final Project project,
      final String name, final Props props) throws ProjectManagerException, IOException {
    final QueryRunner runner = new QueryRunner();
    final String UPDATE_PROPERTIES =
        "UPDATE project_properties SET property=? WHERE project_id=? AND version=? AND name=?";

    final String propertyJSON = PropsUtils.toJSONString(props, true);
    byte[] data = propertyJSON.getBytes("UTF-8");
    if (this.defaultEncodingType == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(data);
    }

    try {
      runner.update(connection, UPDATE_PROPERTIES, data, project.getId(),
          project.getVersion(), name);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error updating property "
          + project.getName() + " version " + project.getVersion(), e);
    }
  }

  private void uploadProjectProperty(final Connection connection, final Project project,
      final String name, final Props props) throws ProjectManagerException, IOException {
    final QueryRunner runner = new QueryRunner();
    final String INSERT_PROPERTIES =
        "INSERT INTO project_properties (project_id, version, name, modified_time, encoding_type, property) values (?,?,?,?,?,?)";

    final String propertyJSON = PropsUtils.toJSONString(props, true);
    byte[] data = propertyJSON.getBytes("UTF-8");
    if (this.defaultEncodingType == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(data);
    }

    try {
      runner.update(connection, INSERT_PROPERTIES, project.getId(),
          project.getVersion(), name, System.currentTimeMillis(),
          this.defaultEncodingType.getNumVal(), data);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error uploading project properties "
          + name + " into " + project.getName() + " version "
          + project.getVersion(), e);
    }
  }

  @Override
  public Props fetchProjectProperty(final int projectId, final int projectVer,
      final String propsName) throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();

    final ProjectPropertiesResultsHandler handler =
        new ProjectPropertiesResultsHandler();
    try {
      final List<Pair<String, Props>> properties =
          runner.query(ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTY,
              handler, projectId, projectVer, propsName);

      if (properties == null || properties.isEmpty()) {
        return null;
      }

      return properties.get(0).getSecond();
    } catch (final SQLException e) {
      logger.error("Error fetching property " + propsName
          + " Project " + projectId + " version " + projectVer, e);
      throw new ProjectManagerException("Error fetching property " + propsName,
          e);
    }
  }

  @Override
  public Props fetchProjectProperty(final Project project, final String propsName)
      throws ProjectManagerException {
    // TODO: 11/23/16 call the other overloaded method fetchProjectProperty internally.
    final QueryRunner runner = createQueryRunner();

    final ProjectPropertiesResultsHandler handler =
        new ProjectPropertiesResultsHandler();
    try {
      final List<Pair<String, Props>> properties =
          runner.query(ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTY,
              handler, project.getId(), project.getVersion(), propsName);

      if (properties == null || properties.isEmpty()) {
        logger.warn("Project " + project.getId() + " version " + project.getVersion()
            + " property " + propsName + " is empty.");
        return null;
      }

      return properties.get(0).getSecond();
    } catch (final SQLException e) {
      logger.error("Error fetching property " + propsName
          + "Project " + project.getId() + " version " + project.getVersion(), e);
      throw new ProjectManagerException("Error fetching property " + propsName
          + "Project " + project.getId() + " version " + project.getVersion(), e);
    }
  }

  @Override
  public void cleanOlderProjectVersion(final int projectId, final int version)
      throws ProjectManagerException {
    final Connection connection = getConnection();

    try {
      cleanOlderProjectVersionFlows(connection, projectId, version);
      cleanOlderProjectVersionProperties(connection, projectId, version);
      cleanOlderProjectFiles(connection, projectId, version);
      cleanOlderProjectVersion(connection, projectId, version);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void cleanOlderProjectVersionFlows(final Connection connection,
      final int projectId, final int version) throws ProjectManagerException {
    final String DELETE_FLOW =
        "DELETE FROM project_flows WHERE project_id=? AND version<?";
    final QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, DELETE_FLOW, projectId, version);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error deleting project version flows "
          + projectId + ":" + version, e);
    }
  }

  private void cleanOlderProjectVersionProperties(final Connection connection,
      final int projectId, final int version) throws ProjectManagerException {
    final String DELETE_PROPERTIES =
        "DELETE FROM project_properties WHERE project_id=? AND version<?";
    final QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, DELETE_PROPERTIES, projectId, version);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error deleting project version properties " + projectId + ":"
              + version, e);
    }
  }

  private void cleanOlderProjectFiles(final Connection connection, final int projectId,
      final int version) throws ProjectManagerException {
    final String DELETE_PROJECT_FILES =
        "DELETE FROM project_files WHERE project_id=? AND version<?";
    final QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, DELETE_PROJECT_FILES, projectId, version);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException("Error deleting project version files "
          + projectId + ":" + version, e);
    }
  }

  private void cleanOlderProjectVersion(final Connection connection, final int projectId,
      final int version) throws ProjectManagerException {
    final String UPDATE_PROJECT_VERSIONS =
        "UPDATE project_versions SET num_chunks=0 WHERE project_id=? AND version<?";
    final QueryRunner runner = new QueryRunner();
    try {
      runner.update(connection, UPDATE_PROJECT_VERSIONS, projectId, version);
      connection.commit();
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error updating project version chunksize " + projectId + ":"
              + version, e);
    }
  }

  @Override
  public Map<String, Props> fetchProjectProperties(final int projectId, final int version)
      throws ProjectManagerException {
    final QueryRunner runner = createQueryRunner();

    final ProjectPropertiesResultsHandler handler =
        new ProjectPropertiesResultsHandler();
    try {
      final List<Pair<String, Props>> properties =
          runner.query(
              ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTIES,
              handler, projectId, version);

      if (properties == null || properties.isEmpty()) {
        return null;
      }

      final HashMap<String, Props> props = new HashMap<>();
      for (final Pair<String, Props> pair : properties) {
        props.put(pair.getFirst(), pair.getSecond());
      }
      return props;
    } catch (final SQLException e) {
      logger.error("Error fetching properties, project id" + projectId + " version " + version, e);
      throw new ProjectManagerException("Error fetching properties", e);
    }
  }

  private Connection getConnection() throws ProjectManagerException {
    Connection connection = null;
    try {
      connection = super.getDBConnection(false);
    } catch (final Exception e) {
      DbUtils.closeQuietly(connection);
      throw new ProjectManagerException("Error getting DB connection.", e);
    }

    return connection;
  }

  private static class ProjectResultHandler implements
      ResultSetHandler<List<Project>> {

    private static final String SELECT_PROJECT_BY_NAME =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE name=?";

    private static final String SELECT_PROJECT_BY_ID =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE id=?";

    private static final String SELECT_ALL_ACTIVE_PROJECTS =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE active=true";

    private static final String SELECT_ACTIVE_PROJECT_BY_NAME =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE name=? AND active=true";

    @Override
    public List<Project> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Project>emptyList();
      }

      final ArrayList<Project> projects = new ArrayList<>();
      do {
        final int id = rs.getInt(1);
        final String name = rs.getString(2);
        final boolean active = rs.getBoolean(3);
        final long modifiedTime = rs.getLong(4);
        final long createTime = rs.getLong(5);
        final int version = rs.getInt(6);
        final String lastModifiedBy = rs.getString(7);
        final String description = rs.getString(8);
        final int encodingType = rs.getInt(9);
        final byte[] data = rs.getBytes(10);

        final Project project;
        if (data != null) {
          final EncodingType encType = EncodingType.fromInteger(encodingType);
          final Object blobObj;
          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              final String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              blobObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              final String jsonString = new String(data, "UTF-8");
              blobObj = JSONUtils.parseJSONFromString(jsonString);
            }
            project = Project.projectFromObject(blobObj);
          } catch (final IOException e) {
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

    private static final String SELECT_PROJECT_PERMISSION =
        "SELECT project_id, modified_time, name, permissions, isGroup FROM project_permissions WHERE project_id=?";

    @Override
    public List<Triple<String, Boolean, Permission>> handle(final ResultSet rs)
        throws SQLException {
      if (!rs.next()) {
        return Collections.<Triple<String, Boolean, Permission>>emptyList();
      }

      final ArrayList<Triple<String, Boolean, Permission>> permissions =
          new ArrayList<>();
      do {
        final String username = rs.getString(3);
        final int permissionFlag = rs.getInt(4);
        final boolean val = rs.getBoolean(5);

        final Permission perm = new Permission(permissionFlag);
        permissions.add(new Triple<>(username, val,
            perm));
      } while (rs.next());

      return permissions;
    }
  }

  private static class ProjectFlowsResultHandler implements
      ResultSetHandler<List<Flow>> {

    private static final String SELECT_PROJECT_FLOW =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json FROM project_flows WHERE project_id=? AND version=? AND flow_id=?";

    private static final String SELECT_ALL_PROJECT_FLOWS =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json FROM project_flows WHERE project_id=? AND version=?";

    @Override
    public List<Flow> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Flow>emptyList();
      }

      final ArrayList<Flow> flows = new ArrayList<>();
      do {
        final String flowId = rs.getString(3);
        final int encodingType = rs.getInt(5);
        final byte[] dataBytes = rs.getBytes(6);

        if (dataBytes == null) {
          continue;
        }

        final EncodingType encType = EncodingType.fromInteger(encodingType);

        Object flowObj = null;
        try {
          // Convoluted way to inflate strings. Should find common package or
          // helper function.
          if (encType == EncodingType.GZIP) {
            // Decompress the sucker.
            final String jsonString = GZIPUtils.unGzipString(dataBytes, "UTF-8");
            flowObj = JSONUtils.parseJSONFromString(jsonString);
          } else {
            final String jsonString = new String(dataBytes, "UTF-8");
            flowObj = JSONUtils.parseJSONFromString(jsonString);
          }

          final Flow flow = Flow.flowFromObject(flowObj);
          flows.add(flow);
        } catch (final IOException e) {
          throw new SQLException("Error retrieving flow data " + flowId, e);
        }

      } while (rs.next());

      return flows;
    }
  }

  private static class ProjectPropertiesResultsHandler implements
      ResultSetHandler<List<Pair<String, Props>>> {

    private static final String SELECT_PROJECT_PROPERTY =
        "SELECT project_id, version, name, modified_time, encoding_type, property FROM project_properties WHERE project_id=? AND version=? AND name=?";

    private static final String SELECT_PROJECT_PROPERTIES =
        "SELECT project_id, version, name, modified_time, encoding_type, property FROM project_properties WHERE project_id=? AND version=?";

    @Override
    public List<Pair<String, Props>> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Pair<String, Props>>emptyList();
      }

      final List<Pair<String, Props>> properties =
          new ArrayList<>();
      do {
        final String name = rs.getString(3);
        final int eventType = rs.getInt(5);
        final byte[] dataBytes = rs.getBytes(6);

        final EncodingType encType = EncodingType.fromInteger(eventType);
        String propertyString = null;

        try {
          if (encType == EncodingType.GZIP) {
            // Decompress the sucker.
            propertyString = GZIPUtils.unGzipString(dataBytes, "UTF-8");
          } else {
            propertyString = new String(dataBytes, "UTF-8");
          }

          final Props props = PropsUtils.fromJSONString(propertyString);
          props.setSource(name);
          properties.add(new Pair<>(name, props));
        } catch (final IOException e) {
          throw new SQLException(e);
        }
      } while (rs.next());

      return properties;
    }
  }

  private static class ProjectLogsResultHandler implements
      ResultSetHandler<List<ProjectLogEvent>> {

    private static final String SELECT_PROJECT_EVENTS_ORDER =
        "SELECT project_id, event_type, event_time, username, message FROM project_events WHERE project_id=? ORDER BY event_time DESC LIMIT ? OFFSET ?";

    @Override
    public List<ProjectLogEvent> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<ProjectLogEvent>emptyList();
      }

      final ArrayList<ProjectLogEvent> events = new ArrayList<>();
      do {
        final int projectId = rs.getInt(1);
        final int eventType = rs.getInt(2);
        final long eventTime = rs.getLong(3);
        final String username = rs.getString(4);
        final String message = rs.getString(5);

        final ProjectLogEvent event =
            new ProjectLogEvent(projectId, EventType.fromInteger(eventType),
                eventTime, username, message);
        events.add(event);
      } while (rs.next());

      return events;
    }
  }

  private static class ProjectFileChunkResultHandler implements
      ResultSetHandler<List<byte[]>> {

    private static final String SELECT_PROJECT_CHUNKS_FILE =
        "SELECT project_id, version, chunk, size, file FROM project_files WHERE project_id=? AND version=? AND chunk >= ? AND chunk < ? ORDER BY chunk ASC";

    @Override
    public List<byte[]> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<byte[]>emptyList();
      }

      final ArrayList<byte[]> data = new ArrayList<>();
      do {
        final byte[] bytes = rs.getBytes(5);

        data.add(bytes);
      } while (rs.next());

      return data;
    }

  }

  private static class ProjectVersionResultHandler implements
      ResultSetHandler<List<ProjectFileHandler>> {

    private static final String SELECT_PROJECT_VERSION =
        "SELECT project_id, version, upload_time, uploader, file_type, file_name, md5, num_chunks, resource_id "
            + "FROM project_versions WHERE project_id=? AND version=?";

    @Override
    public List<ProjectFileHandler> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return null;
      }

      final List<ProjectFileHandler> handlers = new ArrayList<>();
      do {
        final int projectId = rs.getInt(1);
        final int version = rs.getInt(2);
        final long uploadTime = rs.getLong(3);
        final String uploader = rs.getString(4);
        final String fileType = rs.getString(5);
        final String fileName = rs.getString(6);
        final byte[] md5 = rs.getBytes(7);
        final int numChunks = rs.getInt(8);
        final String resourceId = rs.getString(9);

        final ProjectFileHandler handler = new ProjectFileHandler(
            projectId, version, uploadTime, uploader, fileType, fileName, numChunks, md5,
            resourceId);

        handlers.add(handler);
      } while (rs.next());

      return handlers;
    }
  }

  private static class IntHander implements ResultSetHandler<Integer> {

    private static final String SELECT_LATEST_VERSION =
        "SELECT MAX(version) FROM project_versions WHERE project_id=?";

    @Override
    public Integer handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return 0;
      }

      return rs.getInt(1);
    }
  }
}
