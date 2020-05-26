/*
 * Copyright 2017 LinkedIn Corp.
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

import static azkaban.project.JdbcProjectHandlerSet.IntHandler;
import static azkaban.project.JdbcProjectHandlerSet.ProjectFileChunkResultHandler;
import static azkaban.project.JdbcProjectHandlerSet.ProjectFlowsResultHandler;
import static azkaban.project.JdbcProjectHandlerSet.ProjectLogsResultHandler;
import static azkaban.project.JdbcProjectHandlerSet.ProjectPropertiesResultsHandler;
import static azkaban.project.JdbcProjectHandlerSet.ProjectResultHandler;
import static azkaban.project.JdbcProjectHandlerSet.ProjectVersionResultHandler;

import azkaban.Constants.ConfigurationKeys;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseTransOperator;
import azkaban.db.EncodingType;
import azkaban.db.SQLTransaction;
import azkaban.flow.Flow;
import azkaban.project.JdbcProjectHandlerSet.FlowFileResultHandler;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.user.Permission;
import azkaban.user.User;
import azkaban.utils.GZIPUtils;
import azkaban.utils.HashUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * This class implements ProjectLoader using new azkaban-db code to allow DB failover. TODO
 * kunkun-tang: This class is too long. In future, we should split {@link ProjectLoader} interface
 * and have multiple short class implementations.
 */
@Singleton
public class JdbcProjectImpl implements ProjectLoader {

  private static final Logger logger = Logger.getLogger(JdbcProjectImpl.class);

  private static final int CHUCK_SIZE = 1024 * 1024 * 10;
  // Flow yaml files are usually small, set size limitation to 10 MB should be sufficient for now.
  private static final int MAX_FLOW_FILE_SIZE_IN_BYTES = 1024 * 1024 * 10;
  private final DatabaseOperator dbOperator;
  private final File tempDir;
  private final EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  public JdbcProjectImpl(final Props props, final DatabaseOperator databaseOperator) {

    this.dbOperator = databaseOperator;
    this.tempDir = new File(props.getString("project.temp.dir", "temp"));
    if (!this.tempDir.exists()) {
      if (this.tempDir.mkdirs()) {
        logger.info("project temporary folder is being constructed.");
      } else {
        logger.info("project temporary folder already existed.");
      }
    }
  }

  @Override
  public List<Project> fetchAllActiveProjects() throws ProjectManagerException {

    final ProjectResultHandler handler = new ProjectResultHandler();
    List<Project> projects = null;

    try {
      projects = this.dbOperator.query(ProjectResultHandler.SELECT_ALL_ACTIVE_PROJECTS, handler);
    } catch (final SQLException ex) {
      logger.error(ProjectResultHandler.SELECT_ALL_ACTIVE_PROJECTS + " failed.", ex);
      throw new ProjectManagerException("Error retrieving all active projects", ex);
    }
    return projects;
  }

  @Override
  public Project fetchProjectById(final int id) throws ProjectManagerException {

    Project project = null;
    final ProjectResultHandler handler = new ProjectResultHandler();

    try {
      final List<Project> projects = this.dbOperator
          .query(ProjectResultHandler.SELECT_PROJECT_BY_ID, handler, id);
      if (projects.isEmpty()) {
        throw new ProjectManagerException("No project with id " + id + " exists in db.");
      }
      project = projects.get(0);
    } catch (final SQLException ex) {
      logger.error(ProjectResultHandler.SELECT_PROJECT_BY_ID + " failed.", ex);
      throw new ProjectManagerException("Query for existing project failed. Project " + id, ex);
    }

    return project;
  }

  @Override
  public Project fetchProjectByName(final String name) throws ProjectManagerException {
    Project project = null;
    final ProjectResultHandler handler = new ProjectResultHandler();

    // At most one active project with the same name exists in db.
    try {
      final List<Project> projects = this.dbOperator
          .query(ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME, handler, name);
      if (projects.isEmpty()) {
        return null;
      }
      project = projects.get(0);
    } catch (final SQLException ex) {
      logger.error(ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME + " failed.", ex);
      throw new ProjectManagerException(
          ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME + " failed.", ex);
    }
    return project;
  }

  /**
   * Creates a Project in the db.
   * <p>
   * It will throw an exception if it finds an active project of the same name, or the SQL fails
   */
  @Override
  public synchronized Project createNewProject(final String name, final String description,
      final User creator)
      throws ProjectManagerException {
    final ProjectResultHandler handler = new ProjectResultHandler();

    // Check if the same project name exists.
    try {
      final List<Project> projects = this.dbOperator
          .query(ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME, handler, name);
      if (!projects.isEmpty()) {
        throw new ProjectManagerException(
            "Active project with name " + name + " already exists in db.");
      }
    } catch (final SQLException ex) {
      logger.error(ex);
      throw new ProjectManagerException("Checking for existing project failed. " + name, ex);
    }

    final String INSERT_PROJECT =
        "INSERT INTO projects ( name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob) values (?,?,?,?,?,?,?,?,?)";
    final SQLTransaction<Integer> insertProject = transOperator -> {
      final long time = System.currentTimeMillis();
      return transOperator
          .update(INSERT_PROJECT, name, true, time, time, null, creator.getUserId(), description,
              this.defaultEncodingType.getNumVal(), null);
    };

    // Insert project
    try {
      final int numRowsInserted = this.dbOperator.transaction(insertProject);
      if (numRowsInserted == 0) {
        throw new ProjectManagerException("No projects have been inserted.");
      }
    } catch (final SQLException ex) {
      logger.error(INSERT_PROJECT + " failed.", ex);
      throw new ProjectManagerException("Insert project" + name + " for existing project failed. ",
          ex);
    }
    return fetchProjectByName(name);
  }

  @Override
  public void uploadProjectFile(final int projectId, final int version, final File localFile,
      final String uploader, final String uploaderIPAddr)
      throws ProjectManagerException {
    final long startMs = System.currentTimeMillis();
    logger.info(String
        .format("Uploading Project ID: %d file: %s [%d bytes]", projectId, localFile.getName(),
            localFile.length()));

    /*
     * The below transaction uses one connection to do all operations. Ideally, we should commit
     * after the transaction completes. However, uploadFile needs to commit every time when we
     * upload any single chunk.
     *
     * Todo kunkun-tang: fix the transaction issue.
     */
    final SQLTransaction<Integer> uploadProjectFileTransaction = transOperator -> {

      /* Step 1: Update DB with new project info */
      // Database storage does not support thin archives, so we just set the startupDependencies file to null.
      addProjectToProjectVersions(transOperator, projectId, version, localFile, null, uploader,
          computeHash(localFile), null, uploaderIPAddr);
      transOperator.getConnection().commit();

      /* Step 2: Upload File in chunks to DB */
      final int chunks = uploadFileInChunks(transOperator, projectId, version, localFile);

      /* Step 3: Update number of chunks in DB */
      updateChunksInProjectVersions(transOperator, projectId, version, chunks);
      return 1;
    };

    try {
      this.dbOperator.transaction(uploadProjectFileTransaction);
    } catch (final SQLException e) {
      logger.error("upload project files failed.", e);
      throw new ProjectManagerException("upload project files failed.", e);
    }

    final long duration = (System.currentTimeMillis() - startMs) / 1000;
    logger.info(String.format("Uploaded Project ID: %d file: %s [%d bytes] in %d sec", projectId,
        localFile.getName(),
        localFile.length(), duration));
  }


  private byte[] computeHash(final File localFile) {
    logger.info("Creating MD5 hash for upload " + localFile.getName());
    final byte[] md5;
    try {
      md5 = HashUtils.MD5.getHashBytes(localFile);
    } catch (final IOException e) {
      throw new ProjectManagerException("Error getting MD5 hash.", e);
    }

    logger.info("MD5 hash created");
    return md5;
  }

  @Override
  public void addProjectVersion(final int projectId, final int version, final File localFile,
      final File startupDependencies, final String uploader, final byte[] md5,
      final String resourceId, final String uploaderIPAddr) throws ProjectManagerException {

    // when one transaction completes, it automatically commits.
    final SQLTransaction<Integer> transaction = transOperator -> {
      addProjectToProjectVersions(transOperator, projectId, version, localFile,
          startupDependencies, uploader, md5, resourceId, uploaderIPAddr);
      return 1;
    };
    try {
      this.dbOperator.transaction(transaction);
    } catch (final SQLException e) {
      logger.error("addProjectVersion failed.", e);
      throw new ProjectManagerException("addProjectVersion failed.", e);
    }
  }

  /**
   * Insert a new version record to TABLE project_versions before uploading files.
   * <p>
   * The reason for this operation: When error chunking happens in remote mysql server, incomplete
   * file data remains in DB, and an SQL exception is thrown. If we don't have this operation before
   * uploading file, the SQL exception prevents AZ from creating the new version record in Table
   * project_versions. However, the Table project_files still reserve the incomplete files, which
   * causes troubles when uploading a new file: Since the version in TABLE project_versions is still
   * old, mysql will stop inserting new files to db.
   * <p>
   * Why this operation is safe: When AZ uploads a new zip file, it always fetches the latest
   * version proj_v from TABLE project_version, proj_v+1 will be used as the new version for the
   * uploading files.
   * <p>
   * Assume error chunking happens on day 1. proj_v is created for this bad file (old file version +
   * 1). When we upload a new project zip in day2, new file in day 2 will use the new version
   * (proj_v + 1). When file uploading completes, AZ will clean all old chunks in DB afterward.
   */
  private void addProjectToProjectVersions(
      final DatabaseTransOperator transOperator,
      final int projectId,
      final int version,
      final File localFile,
      final File startupDependencies,
      final String uploader,
      final byte[] md5,
      final String resourceId,
      final String uploaderIPAddr) throws ProjectManagerException {
    final long updateTime = System.currentTimeMillis();
    final String INSERT_PROJECT_VERSION = "INSERT INTO project_versions "
        + "(project_id, version, upload_time, uploader, file_type, file_name, md5, num_chunks, resource_id, "
        + "startup_dependencies, uploader_ip_addr) values (?,?,?,?,?,?,?,?,?,?,?)";

    try {
      /*
       * As we don't know the num_chunks before uploading the file, we initialize it to 0,
       * and will update it after uploading completes.
       */
      final String lowercaseFileExtension = FilenameUtils.getExtension(localFile.getName())
          .toLowerCase();

      // Get the startup dependencies input stream (or null if the file does not exist - indicating this is
      // a fat archive).
      final InputStream startupDependenciesStream = getStartupDependenciesInputStream(
          startupDependencies);

      // Perform the DB update
      transOperator.update(INSERT_PROJECT_VERSION, projectId, version, updateTime,
          uploader, lowercaseFileExtension, localFile.getName(), md5, 0, resourceId,
          startupDependenciesStream, uploaderIPAddr);
    } catch (final SQLException e) {
      final String msg = String
          .format("Error initializing project id: %d version: %d ", projectId, version);
      logger.error(msg, e);
      throw new ProjectManagerException(msg, e);
    }
  }

  private InputStream getStartupDependenciesInputStream(final File startupDependencies) {
    try {
      // If startupDependencies is null, we assume this is a fat archive and return null. If it is not null,
      // we assume the file exists and return an input stream for the file.
      return startupDependencies != null ? new FileInputStream(startupDependencies) : null;
    } catch (final FileNotFoundException e) {
      // This shouldn't happen, the file should always exist if it is non-null.
      throw new RuntimeException(e);
    }
  }

  private int uploadFileInChunks(final DatabaseTransOperator transOperator, final int projectId,
      final int version, final File localFile)
      throws ProjectManagerException {

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
          transOperator.update(INSERT_PROJECT_FILES, projectId, version, chunk, size, buf);

          /*
           * We enforce az committing to db when uploading every single chunk,
           * in order to reduce the transaction duration and conserve sql server resources.
           *
           * If the files to be uploaded is very large and we don't commit every single chunk,
           * the remote mysql server will run into memory troubles.
           */
          transOperator.getConnection().commit();
          logger.info("Finished update for " + localFile.getName() + " chunk " + chunk);
        } catch (final SQLException e) {
          throw new ProjectManagerException("Error Chunking during uploading files to db...");
        }
        ++chunk;
        size = bufferedStream.read(buffer);
      }
    } catch (final IOException e) {
      throw new ProjectManagerException(
          String.format(
              "Error chunking file. projectId: %d, version: %d, file:%s[%d bytes], chunk: %d",
              projectId,
              version, localFile.getName(), localFile.length(), chunk));
    } finally {
      IOUtils.closeQuietly(bufferedStream);
    }
    return chunk;
  }

  /**
   * we update num_chunks's actual number to db here.
   */
  private void updateChunksInProjectVersions(final DatabaseTransOperator transOperator,
      final int projectId, final int version, final int chunk)
      throws ProjectManagerException {

    final String UPDATE_PROJECT_NUM_CHUNKS =
        "UPDATE project_versions SET num_chunks=? WHERE project_id=? AND version=?";
    try {
      transOperator.update(UPDATE_PROJECT_NUM_CHUNKS, chunk, projectId, version);
      transOperator.getConnection().commit();
    } catch (final SQLException e) {
      logger.error("Error updating project " + projectId + " : chunk_num " + chunk, e);
      throw new ProjectManagerException(
          "Error updating project " + projectId + " : chunk_num " + chunk, e);
    }
  }

  @Override
  public ProjectFileHandler fetchProjectMetaData(final int projectId, final int version) {
    final ProjectVersionResultHandler pfHandler = new ProjectVersionResultHandler();
    try {
      final List<ProjectFileHandler> projectFiles =
          this.dbOperator
              .query(ProjectVersionResultHandler.SELECT_PROJECT_VERSION, pfHandler, projectId,
                  version);
      if (projectFiles == null || projectFiles.isEmpty()) {
        return null;
      }
      return projectFiles.get(0);
    } catch (final SQLException ex) {
      logger.error("Query for uploaded file for project id " + projectId + " failed.", ex);
      throw new ProjectManagerException(
          "Query for uploaded file for project id " + projectId + " failed.", ex);
    }
  }

  @Override
  public ProjectFileHandler getUploadedFile(final int projectId, final int version)
      throws ProjectManagerException {
    final ProjectFileHandler projHandler = fetchProjectMetaData(projectId, version);
    if (projHandler == null) {
      return null;
    }
    final int numChunks = projHandler.getNumChunks();
    if (numChunks <= 0) {
      throw new ProjectManagerException(String.format("Got numChunks=%s for version %s of project "
              + "%s - seems like this version has been cleaned up already, because enough newer "
              + "versions have been uploaded. To increase the retention of project versions, set "
              + "%s", numChunks, version, projectId,
          ConfigurationKeys.PROJECT_VERSION_RETENTION));
    }
    BufferedOutputStream bStream = null;
    File file;
    try {
      try {
        file = File
            .createTempFile(projHandler.getFileName(), String.valueOf(version), this.tempDir);
        bStream = new BufferedOutputStream(new FileOutputStream(file));
      } catch (final IOException e) {
        throw new ProjectManagerException("Error creating temp file for stream.");
      }

      final int collect = 5;
      int fromChunk = 0;
      int toChunk = collect;
      do {
        final ProjectFileChunkResultHandler chunkHandler = new ProjectFileChunkResultHandler();
        List<byte[]> data = null;
        try {
          data = this.dbOperator
              .query(ProjectFileChunkResultHandler.SELECT_PROJECT_CHUNKS_FILE, chunkHandler,
                  projectId,
                  version, fromChunk, toChunk);
        } catch (final SQLException e) {
          logger.error(e);
          throw new ProjectManagerException("Query for uploaded file for " + projectId + " failed.",
              e);
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
    final byte[] md5;
    try {
      md5 = HashUtils.MD5.getHashBytes(file);
    } catch (final IOException e) {
      throw new ProjectManagerException("Error getting MD5 hash.", e);
    }

    if (Arrays.equals(projHandler.getMD5Hash(), md5)) {
      logger.info("Md5 Hash is valid");
    } else {
      throw new ProjectManagerException(
          String.format("Md5 Hash failed on project %s version %s retrieval of file %s. "
                  + "Expected hash: %s , got hash: %s",
              projHandler.getProjectId(), projHandler.getVersion(), file.getAbsolutePath(),
              Arrays.toString(projHandler.getMD5Hash()), Arrays.toString(md5)));
    }

    projHandler.setLocalFile(file);
    return projHandler;
  }

  @Override
  public void changeProjectVersion(final Project project, final int version, final String user)
      throws ProjectManagerException {
    final long timestamp = System.currentTimeMillis();
    try {
      final String UPDATE_PROJECT_VERSION =
          "UPDATE projects SET version=?,modified_time=?,last_modified_by=? WHERE id=?";

      this.dbOperator.update(UPDATE_PROJECT_VERSION, version, timestamp, user, project.getId());
      project.setVersion(version);
      project.setLastModifiedTimestamp(timestamp);
      project.setLastModifiedUser(user);
    } catch (final SQLException e) {
      logger.error("Error updating switching project version " + project.getName(), e);
      throw new ProjectManagerException(
          "Error updating switching project version " + project.getName(), e);
    }
  }

  @Override
  public void updatePermission(final Project project, final String name, final Permission perm,
      final boolean isGroup)
      throws ProjectManagerException {

    final long updateTime = System.currentTimeMillis();
    try {
      if (this.dbOperator.getDataSource().allowsOnDuplicateKey()) {
        final String INSERT_PROJECT_PERMISSION =
            "INSERT INTO project_permissions (project_id, modified_time, name, permissions, isGroup) values (?,?,?,?,?)"
                + "ON DUPLICATE KEY UPDATE modified_time = VALUES(modified_time), permissions = VALUES(permissions)";
        this.dbOperator
            .update(INSERT_PROJECT_PERMISSION, project.getId(), updateTime, name, perm.toFlags(),
                isGroup);
      } else {
        final String MERGE_PROJECT_PERMISSION =
            "MERGE INTO project_permissions (project_id, modified_time, name, permissions, isGroup) KEY (project_id, name) values (?,?,?,?,?)";
        this.dbOperator
            .update(MERGE_PROJECT_PERMISSION, project.getId(), updateTime, name, perm.toFlags(),
                isGroup);
      }
    } catch (final SQLException ex) {
      logger.error("Error updating project permission", ex);
      throw new ProjectManagerException(
          "Error updating project " + project.getName() + " permissions for " + name, ex);
    }

    if (isGroup) {
      project.setGroupPermission(name, perm);
    } else {
      project.setUserPermission(name, perm);
    }
  }

  @Override
  public void updateProjectSettings(final Project project) throws ProjectManagerException {
    updateProjectSettings(project, this.defaultEncodingType);
  }

  private byte[] convertJsonToBytes(final EncodingType type, final String json) throws IOException {
    byte[] data = json.getBytes("UTF-8");
    if (type == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(data);
    }
    return data;
  }

  private void updateProjectSettings(final Project project, final EncodingType encType)
      throws ProjectManagerException {
    final String UPDATE_PROJECT_SETTINGS = "UPDATE projects SET enc_type=?, settings_blob=? WHERE id=?";

    final String json = JSONUtils.toJSON(project.toObject());
    byte[] data = null;
    try {
      data = convertJsonToBytes(encType, json);
      logger.debug("NumChars: " + json.length() + " Gzip:" + data.length);
    } catch (final IOException e) {
      throw new ProjectManagerException("Failed to encode. ", e);
    }

    try {
      this.dbOperator.update(UPDATE_PROJECT_SETTINGS, encType.getNumVal(), data, project.getId());
    } catch (final SQLException e) {
      logger.error("update Project Settings failed.", e);
      throw new ProjectManagerException(
          "Error updating project " + project.getName() + " version " + project.getVersion(), e);
    }
  }

  @Override
  public void removePermission(final Project project, final String name, final boolean isGroup)
      throws ProjectManagerException {
    final String DELETE_PROJECT_PERMISSION =
        "DELETE FROM project_permissions WHERE project_id=? AND name=? AND isGroup=?";
    try {
      this.dbOperator.update(DELETE_PROJECT_PERMISSION, project.getId(), name, isGroup);
    } catch (final SQLException e) {
      logger.error("remove Permission failed.", e);
      throw new ProjectManagerException(
          "Error deleting project " + project.getName() + " permissions for " + name, e);
    }

    if (isGroup) {
      project.removeGroupPermission(name);
    } else {
      project.removeUserPermission(name);
    }
  }

  /**
   * Todo kunkun-tang: the below implementation doesn't remove a project, but inactivate a project.
   * We should rewrite the code to follow the literal meanings.
   */
  @Override
  public void removeProject(final Project project, final String user)
      throws ProjectManagerException {

    final long updateTime = System.currentTimeMillis();
    final String UPDATE_INACTIVE_PROJECT =
        "UPDATE projects SET active=false,modified_time=?,last_modified_by=? WHERE id=?";
    try {
      this.dbOperator.update(UPDATE_INACTIVE_PROJECT, updateTime, user, project.getId());
    } catch (final SQLException e) {
      logger.error("error remove project " + project.getName(), e);
      throw new ProjectManagerException("Error remove project " + project.getName(), e);
    }
  }

  @Override
  public boolean postEvent(final Project project, final EventType type, final String user,
      final String message) {
    final String INSERT_PROJECT_EVENTS =
        "INSERT INTO project_events (project_id, event_type, event_time, username, message) values (?,?,?,?,?)";
    final long updateTime = System.currentTimeMillis();
    try {
      this.dbOperator
          .update(INSERT_PROJECT_EVENTS, project.getId(), type.getNumVal(), updateTime, user,
              message);
    } catch (final SQLException e) {
      logger.error("post event failed,", e);
      return false;
    }
    return true;
  }

  @Override
  public List<ProjectLogEvent> getProjectEvents(final Project project, final int num,
      final int skip) throws ProjectManagerException {
    final ProjectLogsResultHandler logHandler = new ProjectLogsResultHandler();
    List<ProjectLogEvent> events = null;
    try {
      events = this.dbOperator
          .query(ProjectLogsResultHandler.SELECT_PROJECT_EVENTS_ORDER, logHandler, project.getId(),
              num,
              skip);
    } catch (final SQLException e) {
      logger.error("Error getProjectEvents, project " + project.getName(), e);
      throw new ProjectManagerException("Error getProjectEvents, project " + project.getName(), e);
    }

    return events;
  }

  @Override
  public void updateDescription(final Project project, final String description, final String user)
      throws ProjectManagerException {
    final String UPDATE_PROJECT_DESCRIPTION =
        "UPDATE projects SET description=?,modified_time=?,last_modified_by=? WHERE id=?";
    final long updateTime = System.currentTimeMillis();
    try {
      this.dbOperator
          .update(UPDATE_PROJECT_DESCRIPTION, description, updateTime, user, project.getId());
      project.setDescription(description);
      project.setLastModifiedTimestamp(updateTime);
      project.setLastModifiedUser(user);
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error update Description, project " + project.getName(),
          e);
    }
  }

  @Override
  public int getLatestProjectVersion(final Project project) throws ProjectManagerException {
    final IntHandler handler = new IntHandler();
    try {
      return this.dbOperator.query(IntHandler.SELECT_LATEST_VERSION, handler, project.getId());
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Error marking project " + project.getName() + " as inactive", e);
    }
  }

  @Override
  public void uploadFlows(final Project project, final int version, final Collection<Flow> flows)
      throws ProjectManagerException {
    // We do one at a time instead of batch... because well, the batch could be
    // large.
    logger.info("Uploading flows");
    try {
      for (final Flow flow : flows) {
        uploadFlow(project, version, flow, this.defaultEncodingType);
      }
    } catch (final IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    }
  }

  @Override
  public void uploadFlow(final Project project, final int version, final Flow flow)
      throws ProjectManagerException {
    logger.info("Uploading flow " + flow.getId());
    try {
      uploadFlow(project, version, flow, this.defaultEncodingType);
    } catch (final IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    }
  }

  @Override
  public void updateFlow(final Project project, final int version, final Flow flow)
      throws ProjectManagerException {
    logger.info("Uploading flow " + flow.getId());
    try {
      final String json = JSONUtils.toJSON(flow.toObject());
      final byte[] data = convertJsonToBytes(this.defaultEncodingType, json);
      logger.info("Flow upload " + flow.getId() + " is byte size " + data.length);
      final String UPDATE_FLOW =
          "UPDATE project_flows SET encoding_type=?,json=? WHERE project_id=? AND version=? AND flow_id=?";
      try {
        this.dbOperator
            .update(UPDATE_FLOW, this.defaultEncodingType.getNumVal(), data, project.getId(),
                version, flow.getId());
      } catch (final SQLException e) {
        logger.error("Error inserting flow", e);
        throw new ProjectManagerException("Error inserting flow " + flow.getId(), e);
      }
    } catch (final IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    }
  }

  private void uploadFlow(final Project project, final int version, final Flow flow,
      final EncodingType encType)
      throws ProjectManagerException, IOException {
    final String json = JSONUtils.toJSON(flow.toObject());
    final byte[] data = convertJsonToBytes(encType, json);

    logger.info("Flow upload " + flow.getId() + " is byte size " + data.length);
    final String INSERT_FLOW =
        "INSERT INTO project_flows (project_id, version, flow_id, modified_time, encoding_type, json) values (?,?,?,?,?,?)";
    try {
      this.dbOperator
          .update(INSERT_FLOW, project.getId(), version, flow.getId(), System.currentTimeMillis(),
              encType.getNumVal(), data);
    } catch (final SQLException e) {
      logger.error("Error inserting flow", e);
      throw new ProjectManagerException("Error inserting flow " + flow.getId(), e);
    }
  }

  @Override
  public Flow fetchFlow(final Project project, final String flowId) throws ProjectManagerException {
    throw new UnsupportedOperationException("this method has not been instantiated.");
  }

  @Override
  public List<Flow> fetchAllProjectFlows(final Project project) throws ProjectManagerException {
    return fetchAllFlowsForProjects(Arrays.asList(project)).get(project);
  }

  @Override
  public Map<Project, List<Flow>> fetchAllFlowsForProjects(final List<Project> projects)
      throws ProjectManagerException {
    final SQLTransaction<Map<Project, List<Flow>>> transaction = transOperator -> {
      final Map<Project, List<Flow>> projectToFlows = new HashMap();
      for (final Project p : projects) {
        projectToFlows.put(p, transOperator
            .query(ProjectFlowsResultHandler.SELECT_ALL_PROJECT_FLOWS,
                new ProjectFlowsResultHandler(), p.getId(),
                p.getVersion()));
      }
      return projectToFlows;
    };

    try {
      return this.dbOperator.transaction(transaction);
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error fetching flows for " + projects.size() + " project(s).", e);
    }
  }

  @Override
  public void uploadProjectProperties(final Project project, final List<Props> properties)
      throws ProjectManagerException {
    for (final Props props : properties) {
      try {
        uploadProjectProperty(project, props.getSource(), props);
      } catch (final IOException e) {
        throw new ProjectManagerException("Error uploading project property file", e);
      }
    }
  }

  @Override
  public void uploadProjectProperty(final Project project, final Props props)
      throws ProjectManagerException {
    try {
      uploadProjectProperty(project, props.getSource(), props);
    } catch (final IOException e) {
      throw new ProjectManagerException("Error uploading project property file", e);
    }
  }

  @Override
  public void updateProjectProperty(final Project project, final Props props)
      throws ProjectManagerException {
    try {
      updateProjectProperty(project, props.getSource(), props);
    } catch (final IOException e) {
      throw new ProjectManagerException("Error uploading project property file", e);
    }
  }

  private void updateProjectProperty(final Project project, final String name, final Props props)
      throws ProjectManagerException, IOException {
    final String UPDATE_PROPERTIES =
        "UPDATE project_properties SET property=? WHERE project_id=? AND version=? AND name=?";

    final byte[] propsData = getBytes(props);
    try {
      this.dbOperator
          .update(UPDATE_PROPERTIES, propsData, project.getId(), project.getVersion(), name);
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error updating property " + project.getName() + " version " + project.getVersion(), e);
    }
  }

  private void uploadProjectProperty(final Project project, final String name, final Props props)
      throws ProjectManagerException, IOException {
    final String INSERT_PROPERTIES =
        "INSERT INTO project_properties (project_id, version, name, modified_time, encoding_type, property) values (?,?,?,?,?,?)";

    final byte[] propsData = getBytes(props);
    try {
      this.dbOperator.update(INSERT_PROPERTIES, project.getId(), project.getVersion(), name,
          System.currentTimeMillis(),
          this.defaultEncodingType.getNumVal(), propsData);
    } catch (final SQLException e) {
      throw new ProjectManagerException(
          "Error uploading project properties " + name + " into " + project.getName() + " version "
              + project.getVersion(), e);
    }
  }

  private byte[] getBytes(final Props props) throws IOException {
    final String propertyJSON = PropsUtils.toJSONString(props, true);
    byte[] data = propertyJSON.getBytes("UTF-8");
    if (this.defaultEncodingType == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(data);
    }
    return data;
  }

  @Override
  public Props fetchProjectProperty(final int projectId, final int projectVer,
      final String propsName) throws ProjectManagerException {

    final ProjectPropertiesResultsHandler handler = new ProjectPropertiesResultsHandler();
    try {
      final List<Pair<String, Props>> properties =
          this.dbOperator
              .query(ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTY, handler, projectId,
                  projectVer,
                  propsName);

      if (properties == null || properties.isEmpty()) {
        logger.debug("Project " + projectId + " version " + projectVer + " property " + propsName
            + " is empty.");
        return null;
      }

      return properties.get(0).getSecond();
    } catch (final SQLException e) {
      logger.error("Error fetching property " + propsName + " Project " + projectId + " version "
          + projectVer, e);
      throw new ProjectManagerException("Error fetching property " + propsName, e);
    }
  }

  @Override
  public Props fetchProjectProperty(final Project project, final String propsName)
      throws ProjectManagerException {
    return fetchProjectProperty(project.getId(), project.getVersion(), propsName);
  }

  @Override
  public Map<String, Props> fetchProjectProperties(final int projectId, final int version)
      throws ProjectManagerException {

    try {
      final List<Pair<String, Props>> properties = this.dbOperator
          .query(ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTIES,
              new ProjectPropertiesResultsHandler(), projectId, version);
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

  @Override
  public void cleanOlderProjectVersion(final int projectId, final int version,
      final List<Integer> excludedVersions) throws ProjectManagerException {

    // Would use param of type Array from transOperator.getConnection().createArrayOf() but
    // h2 doesn't support the Array type, so format the filter manually.
    final String EXCLUDED_VERSIONS_FILTER = excludedVersions.stream()
        .map(excluded -> " AND version != " + excluded).collect(Collectors.joining());
    final String VERSION_FILTER = " AND version < ?" + EXCLUDED_VERSIONS_FILTER;

    final String DELETE_FLOW = "DELETE FROM project_flows WHERE project_id=?" + VERSION_FILTER;
    final String DELETE_PROPERTIES =
        "DELETE FROM project_properties WHERE project_id=?" + VERSION_FILTER;
    final String DELETE_PROJECT_FILES =
        "DELETE FROM project_files WHERE project_id=?" + VERSION_FILTER;
    final String UPDATE_PROJECT_VERSIONS =
        "UPDATE project_versions SET num_chunks=0 WHERE project_id=?" + VERSION_FILTER;
    // Todo jamiesjc: delete flow files

    final SQLTransaction<Integer> cleanOlderProjectTransaction = transOperator -> {
      transOperator.update(DELETE_FLOW, projectId, version);
      transOperator.update(DELETE_PROPERTIES, projectId, version);
      transOperator.update(DELETE_PROJECT_FILES, projectId, version);
      return transOperator.update(UPDATE_PROJECT_VERSIONS, projectId, version);
    };

    try {
      final int res = this.dbOperator.transaction(cleanOlderProjectTransaction);
      if (res == 0) {
        logger.info("clean older project given project id " + projectId + " doesn't take effect.");
      }
    } catch (final SQLException e) {
      logger.error("clean older project transaction failed", e);
      throw new ProjectManagerException("clean older project transaction failed", e);
    }
  }

  @Override
  public void uploadFlowFile(final int projectId, final int projectVersion, final File flowFile,
      final int flowVersion) throws ProjectManagerException {
    logger.info(String
        .format(
            "Uploading flow file %s, version %d for project %d, version %d, file length is [%d bytes]",
            flowFile.getName(), flowVersion, projectId, projectVersion, flowFile.length()));

    if (flowFile.length() > MAX_FLOW_FILE_SIZE_IN_BYTES) {
      throw new ProjectManagerException("Flow file length exceeds 10 MB limit.");
    }

    final byte[] buffer = new byte[MAX_FLOW_FILE_SIZE_IN_BYTES];
    final String INSERT_FLOW_FILES =
        "INSERT INTO project_flow_files (project_id, project_version, flow_name, flow_version, "
            + "modified_time, "
            + "flow_file) values (?,?,?,?,?,?)";

    try (final FileInputStream input = new FileInputStream(flowFile);
        final BufferedInputStream bufferedStream = new BufferedInputStream(input)) {
      final int size = bufferedStream.read(buffer);
      logger.info("Read bytes for " + flowFile.getName() + ", size:" + size);
      final byte[] buf = Arrays.copyOfRange(buffer, 0, size);
      try {
        this.dbOperator
            .update(INSERT_FLOW_FILES, projectId, projectVersion, flowFile.getName(), flowVersion,
                System.currentTimeMillis(), buf);
      } catch (final SQLException e) {
        throw new ProjectManagerException(
            "Error uploading flow file " + flowFile.getName() + ", version " + flowVersion + ".",
            e);
      }
    } catch (final IOException e) {
      throw new ProjectManagerException(
          String.format(
              "Error reading flow file %s, version: %d, length: [%d bytes].",
              flowFile.getName(), flowVersion, flowFile.length()));
    }
  }

  @Override
  public File getUploadedFlowFile(final int projectId, final int projectVersion,
      final String flowFileName, final int flowVersion, final File tempDir)
      throws ProjectManagerException, IOException {
    final FlowFileResultHandler handler = new FlowFileResultHandler();

    final List<byte[]> data;
    // Created separate temp directory for each flow file to avoid overwriting the same file by
    // multiple threads concurrently. Flow file name will be interpret as the flow name when
    // parsing the yaml flow file, so it has to be specific.
    final File file = new File(tempDir, flowFileName);
    try (final FileOutputStream output = new FileOutputStream(file);
        final BufferedOutputStream bufferedStream = new BufferedOutputStream(output)) {
      try {
        data = this.dbOperator
            .query(FlowFileResultHandler.SELECT_FLOW_FILE, handler,
                projectId, projectVersion, flowFileName, flowVersion);
      } catch (final SQLException e) {
        throw new ProjectManagerException(
            "Failed to query uploaded flow file for project " + projectId + " version "
                + projectVersion + ", flow file " + flowFileName + " version " + flowVersion, e);
      }

      if (data == null || data.isEmpty()) {
        throw new ProjectManagerException(
            "No flow file could be found in DB table for project " + projectId + " version " +
                projectVersion + ", flow file " + flowFileName + " version " + flowVersion);
      }
      bufferedStream.write(data.get(0));
    } catch (final IOException e) {
      throw new ProjectManagerException(
          "Error writing to output stream for project " + projectId + " version " + projectVersion
              + ", flow file " + flowFileName + " version " + flowVersion, e);
    }
    return file;
  }

  @Override
  public int getLatestFlowVersion(final int projectId, final int projectVersion,
      final String flowName) throws ProjectManagerException {
    final IntHandler handler = new IntHandler();
    try {
      return this.dbOperator.query(IntHandler.SELECT_LATEST_FLOW_VERSION, handler, projectId,
          projectVersion, flowName);
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException(
          "Error selecting latest flow version from project " + projectId + ", version " +
              projectVersion + ", flow " + flowName + ".", e);
    }
  }

  @Override
  public boolean isFlowFileUploaded(final int projectId, final int projectVersion)
      throws ProjectManagerException {
    final FlowFileResultHandler handler = new FlowFileResultHandler();
    final List<byte[]> data;

    try {
      data = this.dbOperator
          .query(FlowFileResultHandler.SELECT_ALL_FLOW_FILES, handler,
              projectId, projectVersion);
    } catch (final SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Failed to query uploaded flow files ", e);
    }

    return !data.isEmpty();
  }

  /**
   * List of Projects with ids that match with any id specified in the list
   *
   * @param ids List of ids of projects to be queried
   * @throws ProjectManagerException
   */
  @Override
  public List<Project> fetchProjectById(final List<Integer> ids) throws ProjectManagerException {
    if (ids.size() == 0) {
      throw new ProjectManagerException("No matching ids to query");
    }
    final ProjectResultHandler handler = new ProjectResultHandler();
    final List<Project> projects;
    final String name = StringUtils.join(ids, ',').toString();
    final String SELECT_ACTIVE_PROJECT_BY_IDS = "SELECT "
        + "prj.id, prj.name, prj.active, prj.modified_time, prj.create_time, prj.version, prj.last_modified_by, prj.description, prj.enc_type, prj.settings_blob, "
        + "prm.name, prm.permissions, prm.isGroup "
        + "FROM projects prj "
        + "LEFT JOIN project_permissions prm ON prj.id = prm.project_id WHERE prj.id in (" + name
        + ") and prj.active = true; ";
    try {
      projects = this.dbOperator
          .query(SELECT_ACTIVE_PROJECT_BY_IDS, handler);
      if (projects.isEmpty()) {
        return null;
      }
    } catch (final SQLException ex) {
      logger.error(SELECT_ACTIVE_PROJECT_BY_IDS + " failed.", ex);
      throw new ProjectManagerException(
          SELECT_ACTIVE_PROJECT_BY_IDS + " failed.", ex);
    }
    return projects;
  }
}
