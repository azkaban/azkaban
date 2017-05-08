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

import azkaban.database.EncodingType;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseTransOperator;
import azkaban.db.SQLTransaction;
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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import static azkaban.project.JdbcProjectHandlerSet.*;


/**
 * This class implements ProjectLoader using new azkaban-db code to allow DB failover.
 * TODO kunkun-tang: This class is too long. In future, we should split {@link ProjectLoader} interface
 * and have multiple short class implementations.
 */
public class JdbcProjectImpl implements ProjectLoader {
  private static final Logger logger = Logger.getLogger(JdbcProjectImpl.class);

  private static final int CHUCK_SIZE = 1024 * 1024 * 10;
  private File tempDir;
  private final DatabaseOperator dbOperator;

  private EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  public JdbcProjectImpl(Props props, DatabaseOperator databaseOperator) {

    this.dbOperator = databaseOperator;
    tempDir = new File(props.getString("project.temp.dir", "temp"));
    if (!tempDir.exists()) {
      if (tempDir.mkdirs()) {
        logger.info("project temporary folder is being constructed.");
      } else {
        logger.info("project temporary folder already existed.");
      }
    }
  }

  @Override
  public List<Project> fetchAllActiveProjects() throws ProjectManagerException {

    ProjectResultHandler handler = new ProjectResultHandler();
    List<Project> projects = null;

    try {
      projects = dbOperator.query(ProjectResultHandler.SELECT_ALL_ACTIVE_PROJECTS, handler);
      projects.forEach(project -> {
        for (Triple<String, Boolean, Permission> perm : fetchPermissionsForProject(project)) {
          setProjectPermission(project, perm);
        }
      });
    } catch (SQLException ex) {
      logger.error(ProjectResultHandler.SELECT_PROJECT_BY_ID + " failed.", ex);
      throw new ProjectManagerException("Error retrieving all projects", ex);
    }
    return projects;
  }

  private void setProjectPermission(Project project, Triple<String, Boolean, Permission> perm) {
    if (perm.getSecond()) {
      project.setGroupPermission(perm.getFirst(), perm.getThird());
    } else {
      project.setUserPermission(perm.getFirst(), perm.getThird());
    }
  }

  @Override
  public Project fetchProjectById(int id) throws ProjectManagerException {

    Project project = null;
    ProjectResultHandler handler = new ProjectResultHandler();

    try {
      List<Project> projects = dbOperator.query(ProjectResultHandler.SELECT_PROJECT_BY_ID, handler, id);
      if (projects.isEmpty()) {
        throw new ProjectManagerException("No project with id " + id + " exists in db.");
      }
      project = projects.get(0);

      // Fetch the user permissions
      for (Triple<String, Boolean, Permission> perm : fetchPermissionsForProject(project)) {
        // TODO kunkun-tang: understand why we need to check permission not equal to 0 here.
        if (perm.getThird().toFlags() != 0) {
          setProjectPermission(project, perm);
        }
      }
    } catch (SQLException ex) {
      logger.error(ProjectResultHandler.SELECT_PROJECT_BY_ID + " failed.", ex);
      throw new ProjectManagerException("Query for existing project failed. Project " + id, ex);
    }

    return project;
  }

  @Override
  public Project fetchProjectByName(String name) throws ProjectManagerException {
    Project project = null;
    ProjectResultHandler handler = new ProjectResultHandler();

    // select active project from db first, if not exist, select inactive one.
    // At most one active project with the same name exists in db.
    try {
      List<Project> projects = dbOperator.query(ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME, handler, name);
      if (projects.isEmpty()) {
        projects = dbOperator.query(ProjectResultHandler.SELECT_PROJECT_BY_NAME, handler, name);
        if (projects.isEmpty()) {
          throw new ProjectManagerException("No project with name " + name + " exists in db.");
        }
      }
      project = projects.get(0);
      for (Triple<String, Boolean, Permission> perm : fetchPermissionsForProject(project)) {
        if (perm.getThird().toFlags() != 0) {
          setProjectPermission(project, perm);
        }
      }
    } catch (SQLException ex) {
      logger.error(ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME + " failed.", ex);
      throw new ProjectManagerException(ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME + " failed.", ex);
    }
    return project;
  }

  private List<Triple<String, Boolean, Permission>> fetchPermissionsForProject(Project project)
      throws ProjectManagerException {
    ProjectPermissionsResultHandler permHander = new ProjectPermissionsResultHandler();

    List<Triple<String, Boolean, Permission>> permissions = null;
    try {
      permissions =
          dbOperator.query(ProjectPermissionsResultHandler.SELECT_PROJECT_PERMISSION, permHander, project.getId());
    } catch (SQLException ex) {
      logger.error(ProjectPermissionsResultHandler.SELECT_PROJECT_PERMISSION + " failed.", ex);
      throw new ProjectManagerException("Query for permissions for " + project.getName() + " failed.", ex);
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
  public synchronized Project createNewProject(String name, String description, User creator)
      throws ProjectManagerException {
    ProjectResultHandler handler = new ProjectResultHandler();

    // Check if the same project name exists.
    try {
      List<Project> projects = dbOperator.query(ProjectResultHandler.SELECT_ACTIVE_PROJECT_BY_NAME, handler, name);
      if (!projects.isEmpty()) {
        throw new ProjectManagerException("Active project with name " + name + " already exists in db.");
      }
    } catch (SQLException ex) {
      logger.error(ex);
      throw new ProjectManagerException("Checking for existing project failed. " + name, ex);
    }

    final String INSERT_PROJECT =
        "INSERT INTO projects ( name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob) values (?,?,?,?,?,?,?,?,?)";
    SQLTransaction<Integer> insertProject = transOperator -> {
      long time = System.currentTimeMillis();
      return transOperator.update(INSERT_PROJECT, name, true, time, time, null, creator.getUserId(), description,
          defaultEncodingType.getNumVal(), null);
    };

    // Insert project
    try {
      int numRowsInserted = dbOperator.transaction(insertProject);
      if (numRowsInserted == 0) {
        throw new ProjectManagerException("No projects have been inserted.");
      }
    } catch (SQLException ex) {
      logger.error(INSERT_PROJECT + " failed.", ex);
      throw new ProjectManagerException("Insert project" + name + " for existing project failed. ", ex);
    }
    return fetchProjectByName(name);
  }

  @Override
  public void uploadProjectFile(int projectId, int version, File localFile, String uploader)
      throws ProjectManagerException {
    long startMs = System.currentTimeMillis();
    logger.info(String.format("Uploading Project ID: %d file: %s [%d bytes]", projectId, localFile.getName(),
        localFile.length()));

    /*
     * The below transaction uses one connection to do all operations. Ideally, we should commit
     * after the transaction completes. However, uploadFile needs to commit every time when we
     * upload any single chunk.
     *
     * Todo kunkun-tang: fix the transaction issue.
     */
    SQLTransaction<Integer> uploadProjectFileTransaction = transOperator -> {

      /* Step 1: Update DB with new project info */
      addProjectToProjectVersions(transOperator, projectId, version, localFile, uploader, null);
      transOperator.getConnection().commit();

      /* Step 2: Upload File in chunks to DB */
      int chunks = uploadFileInChunks(transOperator, projectId, version, localFile);

      /* Step 3: Update number of chunks in DB */
      updateChunksInProjectVersions(transOperator, projectId, version, chunks);
      return 1;
    };

    try {
      dbOperator.transaction(uploadProjectFileTransaction, false);
    } catch (SQLException e) {
      logger.error("upload project files failed.", e);
      throw new ProjectManagerException("upload project files failed.", e);
    }

    long duration = (System.currentTimeMillis() - startMs) / 1000;
    logger.info(String.format("Uploaded Project ID: %d file: %s [%d bytes] in %d sec", projectId, localFile.getName(),
        localFile.length(), duration));
  }


  @Override
  public void addProjectVersion(int projectId, int version, File localFile, String uploader, String resourceId)
      throws ProjectManagerException {

    // when one transaction completes, it automatically commits.
    SQLTransaction<Integer> transaction = transOperator -> {
      addProjectToProjectVersions(transOperator, projectId, version, localFile, uploader, resourceId);
      return 1;
    };
    try {
      dbOperator.transaction(transaction);
    } catch (SQLException e) {
      logger.error("addProjectVersion failed.", e);
      throw new ProjectManagerException("addProjectVersion failed.", e);
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
  private void addProjectToProjectVersions(DatabaseTransOperator transOperator, int projectId, int version, File localFile, String uploader,
      String resourceId) throws ProjectManagerException {
    final long updateTime = System.currentTimeMillis();
    logger.info("Creating message digest for upload " + localFile.getName());
    byte[] md5;
    try {
      md5 = Md5Hasher.md5Hash(localFile);
    } catch (IOException e) {
      throw new ProjectManagerException("Error getting md5 hash.", e);
    }

    logger.info("Md5 hash created");

    final String INSERT_PROJECT_VERSION = "INSERT INTO project_versions "
        + "(project_id, version, upload_time, uploader, file_type, file_name, md5, num_chunks, resource_id) values "
        + "(?,?,?,?,?,?,?,?,?)";

    try {
      /*
       * As we don't know the num_chunks before uploading the file, we initialize it to 0,
       * and will update it after uploading completes.
       */
      transOperator.update(INSERT_PROJECT_VERSION, projectId, version, updateTime, uploader,
          Files.getFileExtension(localFile.getName()), localFile.getName(), md5, 0, resourceId);
    } catch (SQLException e) {
      String msg = String.format("Error initializing project id: %d version: %d ", projectId, version);
      logger.error(msg, e);
      throw new ProjectManagerException(msg, e);
    }
  }

  private int uploadFileInChunks(DatabaseTransOperator transOperator, int projectId, int version, File localFile)
      throws ProjectManagerException {

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
        } catch (SQLException e) {
          throw new ProjectManagerException("Error Chunking during uploading files to db...");
        }
        ++chunk;
        size = bufferedStream.read(buffer);
      }
    } catch (IOException e) {
      throw new ProjectManagerException(
          String.format("Error chunking file. projectId: %d, version: %d, file:%s[%d bytes], chunk: %d", projectId,
              version, localFile.getName(), localFile.length(), chunk));
    } finally {
      IOUtils.closeQuietly(bufferedStream);
    }
    return chunk;
  }

  /**
   * we update num_chunks's actual number to db here.
   */
  private void updateChunksInProjectVersions(DatabaseTransOperator transOperator, int projectId, int version, int chunk)
      throws ProjectManagerException {

    final String UPDATE_PROJECT_NUM_CHUNKS =
        "UPDATE project_versions SET num_chunks=? WHERE project_id=? AND version=?";
    try {
      transOperator.update(UPDATE_PROJECT_NUM_CHUNKS, chunk, projectId, version);
      transOperator.getConnection().commit();
    } catch (SQLException e) {
      logger.error("Error updating project " + projectId + " : chunk_num " + chunk, e);
      throw new ProjectManagerException("Error updating project " + projectId + " : chunk_num " + chunk, e);
    }
  }

  @Override
  public ProjectFileHandler fetchProjectMetaData(int projectId, int version) {
    ProjectVersionResultHandler pfHandler = new ProjectVersionResultHandler();
    try {
      List<ProjectFileHandler> projectFiles =
          dbOperator.query(ProjectVersionResultHandler.SELECT_PROJECT_VERSION, pfHandler, projectId, version);
      if (projectFiles == null || projectFiles.isEmpty()) {
        return null;
      }
      return projectFiles.get(0);
    } catch (SQLException ex) {
      logger.error("Query for uploaded file for project id " + projectId + " failed.", ex);
      throw new ProjectManagerException("Query for uploaded file for project id " + projectId + " failed.", ex);
    }
  }

  @Override
  public ProjectFileHandler getUploadedFile(int projectId, int version) throws ProjectManagerException {
    ProjectFileHandler projHandler = fetchProjectMetaData(projectId, version);
    if (projHandler == null) {
      return null;
    }
    int numChunks = projHandler.getNumChunks();
    BufferedOutputStream bStream = null;
    File file;
    try {
      try {
        file = File.createTempFile(projHandler.getFileName(), String.valueOf(version), tempDir);
        bStream = new BufferedOutputStream(new FileOutputStream(file));
      } catch (IOException e) {
        throw new ProjectManagerException("Error creating temp file for stream.");
      }

      int collect = 5;
      int fromChunk = 0;
      int toChunk = collect;
      do {
        ProjectFileChunkResultHandler chunkHandler = new ProjectFileChunkResultHandler();
        List<byte[]> data = null;
        try {
          data = dbOperator.query(ProjectFileChunkResultHandler.SELECT_PROJECT_CHUNKS_FILE, chunkHandler, projectId,
              version, fromChunk, toChunk);
        } catch (SQLException e) {
          logger.error(e);
          throw new ProjectManagerException("Query for uploaded file for " + projectId + " failed.", e);
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
  public void changeProjectVersion(Project project, int version, String user) throws ProjectManagerException {
    long timestamp = System.currentTimeMillis();
    try {
      final String UPDATE_PROJECT_VERSION =
          "UPDATE projects SET version=?,modified_time=?,last_modified_by=? WHERE id=?";

      dbOperator.update(UPDATE_PROJECT_VERSION, version, timestamp, user, project.getId());
      project.setVersion(version);
      project.setLastModifiedTimestamp(timestamp);
      project.setLastModifiedUser(user);
    } catch (SQLException e) {
      logger.error("Error updating switching project version " + project.getName(), e);
      throw new ProjectManagerException("Error updating switching project version " + project.getName(), e);
    }
  }

  @Override
  public void updatePermission(Project project, String name, Permission perm, boolean isGroup)
      throws ProjectManagerException {

    long updateTime = System.currentTimeMillis();
    try {
      if (dbOperator.getDataSource().allowsOnDuplicateKey()) {
        final String INSERT_PROJECT_PERMISSION =
            "INSERT INTO project_permissions (project_id, modified_time, name, permissions, isGroup) values (?,?,?,?,?)"
                + "ON DUPLICATE KEY UPDATE modified_time = VALUES(modified_time), permissions = VALUES(permissions)";
        dbOperator.update(INSERT_PROJECT_PERMISSION, project.getId(), updateTime, name, perm.toFlags(), isGroup);
      } else {
        final String MERGE_PROJECT_PERMISSION =
            "MERGE INTO project_permissions (project_id, modified_time, name, permissions, isGroup) KEY (project_id, name) values (?,?,?,?,?)";
        dbOperator.update(MERGE_PROJECT_PERMISSION, project.getId(), updateTime, name, perm.toFlags(), isGroup);
      }
    } catch (SQLException ex) {
      logger.error("Error updating project permission", ex);
      throw new ProjectManagerException("Error updating project " + project.getName() + " permissions for " + name, ex);
    }

    if (isGroup) {
      project.setGroupPermission(name, perm);
    } else {
      project.setUserPermission(name, perm);
    }
  }

  @Override
  public void updateProjectSettings(Project project) throws ProjectManagerException {
    updateProjectSettings(project, defaultEncodingType);
  }

  private byte[] convertJsonToBytes(EncodingType type, String json) throws IOException {
    byte[] data = json.getBytes("UTF-8");
    if (type == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(data);
    }
    return data;
  }

  private void updateProjectSettings(Project project, EncodingType encType) throws ProjectManagerException {
    final String UPDATE_PROJECT_SETTINGS = "UPDATE projects SET enc_type=?, settings_blob=? WHERE id=?";

    String json = JSONUtils.toJSON(project.toObject());
    byte[] data = null;
    try {
      data = convertJsonToBytes(encType, json);
      logger.debug("NumChars: " + json.length() + " Gzip:" + data.length);
    } catch (IOException e) {
      throw new ProjectManagerException("Failed to encode. ", e);
    }

    try {
      dbOperator.update(UPDATE_PROJECT_SETTINGS, encType.getNumVal(), data, project.getId());
    } catch (SQLException e) {
      logger.error("update Project Settings failed.", e);
      throw new ProjectManagerException(
          "Error updating project " + project.getName() + " version " + project.getVersion(), e);
    }
  }

  @Override
  public void removePermission(Project project, String name, boolean isGroup) throws ProjectManagerException {
    final String DELETE_PROJECT_PERMISSION =
        "DELETE FROM project_permissions WHERE project_id=? AND name=? AND isGroup=?";
    try {
      dbOperator.update(DELETE_PROJECT_PERMISSION, project.getId(), name, isGroup);
    } catch (SQLException e) {
      logger.error("remove Permission failed.", e);
      throw new ProjectManagerException("Error deleting project " + project.getName() + " permissions for " + name, e);
    }

    if (isGroup) {
      project.removeGroupPermission(name);
    } else {
      project.removeUserPermission(name);
    }
  }

  @Override
  public List<Triple<String, Boolean, Permission>> getProjectPermissions(Project project) throws ProjectManagerException {
    return fetchPermissionsForProject(project);
  }

  /**
   * Todo kunkun-tang: the below implementation inactivate a project.
   * We probably should remove one project.
   */
  @Override
  public void removeProject(Project project, String user) throws ProjectManagerException {

    long updateTime = System.currentTimeMillis();
    final String UPDATE_INACTIVE_PROJECT =
        "UPDATE projects SET active=false,modified_time=?,last_modified_by=? WHERE id=?";
    try {
      dbOperator.update(UPDATE_INACTIVE_PROJECT, updateTime, user, project.getId());
    } catch (SQLException e) {
      logger.error("error remove project " + project.getName(), e);
      throw new ProjectManagerException("Error remove project " + project.getName(), e);
    }
  }

  @Override
  public boolean postEvent(Project project, EventType type, String user, String message) {
    final String INSERT_PROJECT_EVENTS =
        "INSERT INTO project_events (project_id, event_type, event_time, username, message) values (?,?,?,?,?)";
    long updateTime = System.currentTimeMillis();
    try {
      dbOperator.update(INSERT_PROJECT_EVENTS, project.getId(), type.getNumVal(), updateTime, user, message);
    } catch (SQLException e) {
      logger.error("post event failed,", e);
      return false;
    }
    return true;
  }

  @Override
  public List<ProjectLogEvent> getProjectEvents(Project project, int num, int skip) throws ProjectManagerException {
    ProjectLogsResultHandler logHandler = new ProjectLogsResultHandler();
    List<ProjectLogEvent> events = null;
    try {
      events = dbOperator.query(ProjectLogsResultHandler.SELECT_PROJECT_EVENTS_ORDER, logHandler, project.getId(), num,
          skip);
    } catch (SQLException e) {
      logger.error("Error getProjectEvents, project " + project.getName(), e);
      throw new ProjectManagerException("Error getProjectEvents, project " + project.getName(), e);
    }

    return events;
  }

  @Override
  public void updateDescription(Project project, String description, String user) throws ProjectManagerException {
    final String UPDATE_PROJECT_DESCRIPTION =
        "UPDATE projects SET description=?,modified_time=?,last_modified_by=? WHERE id=?";
    long updateTime = System.currentTimeMillis();
    try {
      dbOperator.update(UPDATE_PROJECT_DESCRIPTION, description, updateTime, user, project.getId());
      project.setDescription(description);
      project.setLastModifiedTimestamp(updateTime);
      project.setLastModifiedUser(user);
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error update Description, project " + project.getName(), e);
    }
  }

  @Override
  public int getLatestProjectVersion(Project project) throws ProjectManagerException {
    IntHander handler = new IntHander();
    try {
      return dbOperator.query(IntHander.SELECT_LATEST_VERSION, handler, project.getId());
    } catch (SQLException e) {
      logger.error(e);
      throw new ProjectManagerException("Error marking project " + project.getName() + " as inactive", e);
    }
  }

  @Override
  public void uploadFlows(Project project, int version, Collection<Flow> flows) throws ProjectManagerException {
    // We do one at a time instead of batch... because well, the batch could be
    // large.
    logger.info("Uploading flows");
    try {
      for (Flow flow : flows) {
        uploadFlow(project, version, flow, defaultEncodingType);
      }
    } catch (IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    }
  }

  @Override
  public void uploadFlow(Project project, int version, Flow flow) throws ProjectManagerException {
    logger.info("Uploading flow " + flow.getId());
    try {
      uploadFlow(project, version, flow, defaultEncodingType);
    } catch (IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    }
  }

  @Override
  public void updateFlow(Project project, int version, Flow flow) throws ProjectManagerException {
    logger.info("Uploading flow " + flow.getId());
    try {
      String json = JSONUtils.toJSON(flow.toObject());
      byte[] data = convertJsonToBytes(defaultEncodingType, json);
      logger.info("Flow upload " + flow.getId() + " is byte size " + data.length);
      final String UPDATE_FLOW =
          "UPDATE project_flows SET encoding_type=?,json=? WHERE project_id=? AND version=? AND flow_id=?";
      try {
        dbOperator.update(UPDATE_FLOW, defaultEncodingType.getNumVal(), data, project.getId(), version, flow.getId());
      } catch (SQLException e) {
        logger.error("Error inserting flow", e);
        throw new ProjectManagerException("Error inserting flow " + flow.getId(), e);
      }
    } catch (IOException e) {
      throw new ProjectManagerException("Flow Upload failed.", e);
    }
  }

  private void uploadFlow(Project project, int version, Flow flow, EncodingType encType)
      throws ProjectManagerException, IOException {
    String json = JSONUtils.toJSON(flow.toObject());
    byte[] data = convertJsonToBytes(encType, json);

    logger.info("Flow upload " + flow.getId() + " is byte size " + data.length);
    final String INSERT_FLOW =
        "INSERT INTO project_flows (project_id, version, flow_id, modified_time, encoding_type, json) values (?,?,?,?,?,?)";
    try {
      dbOperator.update(INSERT_FLOW, project.getId(), version, flow.getId(), System.currentTimeMillis(),
          encType.getNumVal(), data);
    } catch (SQLException e) {
      logger.error("Error inserting flow", e);
      throw new ProjectManagerException("Error inserting flow " + flow.getId(), e);
    }
  }

  @Override
  public Flow fetchFlow(Project project, String flowId) throws ProjectManagerException {
    throw new UnsupportedOperationException("this method has not been instantiated.");
  }

  @Override
  public List<Flow> fetchAllProjectFlows(Project project) throws ProjectManagerException {
    ProjectFlowsResultHandler handler = new ProjectFlowsResultHandler();
    List<Flow> flows = null;
    try {
      flows = dbOperator.query(ProjectFlowsResultHandler.SELECT_ALL_PROJECT_FLOWS, handler, project.getId(),
          project.getVersion());
    } catch (SQLException e) {
      throw new ProjectManagerException(
          "Error fetching flows from project " + project.getName() + " version " + project.getVersion(), e);
    }
    return flows;
  }

  @Override
  public void uploadProjectProperties(Project project, List<Props> properties) throws ProjectManagerException {
    for (Props props : properties) {
      try {
        uploadProjectProperty(project, props.getSource(), props);
      } catch (IOException e) {
        throw new ProjectManagerException("Error uploading project property file", e);
      }
    }
  }

  @Override
  public void uploadProjectProperty(Project project, Props props) throws ProjectManagerException {
    try {
      uploadProjectProperty(project, props.getSource(), props);
    } catch (IOException e) {
      throw new ProjectManagerException("Error uploading project property file", e);
    }
  }

  @Override
  public void updateProjectProperty(Project project, Props props) throws ProjectManagerException {
    try {
      updateProjectProperty(project, props.getSource(), props);
    } catch (IOException e) {
      throw new ProjectManagerException("Error uploading project property file", e);
    }
  }

  private void updateProjectProperty(Project project, String name, Props props)
      throws ProjectManagerException, IOException {
    final String UPDATE_PROPERTIES =
        "UPDATE project_properties SET property=? WHERE project_id=? AND version=? AND name=?";

    byte[] propsData = getBytes(props);
    try {
      dbOperator.update(UPDATE_PROPERTIES, propsData, project.getId(), project.getVersion(), name);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          "Error updating property " + project.getName() + " version " + project.getVersion(), e);
    }
  }

  private void uploadProjectProperty(Project project, String name, Props props)
      throws ProjectManagerException, IOException {
    final String INSERT_PROPERTIES =
        "INSERT INTO project_properties (project_id, version, name, modified_time, encoding_type, property) values (?,?,?,?,?,?)";

    byte[] propsData = getBytes(props);
    try {
      dbOperator.update(INSERT_PROPERTIES, project.getId(), project.getVersion(), name, System.currentTimeMillis(),
          defaultEncodingType.getNumVal(), propsData);
    } catch (SQLException e) {
      throw new ProjectManagerException(
          "Error uploading project properties " + name + " into " + project.getName() + " version "
              + project.getVersion(), e);
    }
  }

  private byte[] getBytes(Props props) throws IOException {
    String propertyJSON = PropsUtils.toJSONString(props, true);
    byte[] data = propertyJSON.getBytes("UTF-8");
    if (defaultEncodingType == EncodingType.GZIP) {
      data = GZIPUtils.gzipBytes(data);
    }
    return data;
  }

  @Override
  public Props fetchProjectProperty(int projectId, int projectVer, String propsName) throws ProjectManagerException {

    ProjectPropertiesResultsHandler handler = new ProjectPropertiesResultsHandler();
    try {
      List<Pair<String, Props>> properties =
          dbOperator.query(ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTY, handler, projectId, projectVer,
              propsName);

      if (properties == null || properties.isEmpty()) {
        logger.warn("Project " + projectId + " version " + projectVer + " property " + propsName + " is empty.");
        return null;
      }

      return properties.get(0).getSecond();
    } catch (SQLException e) {
      logger.error("Error fetching property " + propsName + " Project " + projectId + " version " + projectVer, e);
      throw new ProjectManagerException("Error fetching property " + propsName, e);
    }
  }

  @Override
  public Props fetchProjectProperty(Project project, String propsName) throws ProjectManagerException {
    return fetchProjectProperty(project.getId(), project.getVersion(), propsName);
  }

  @Override
  public Map<String, Props> fetchProjectProperties(int projectId, int version) throws ProjectManagerException {

    try {
      List<Pair<String, Props>> properties = dbOperator.query(ProjectPropertiesResultsHandler.SELECT_PROJECT_PROPERTIES,
          new ProjectPropertiesResultsHandler(), projectId, version);
      if (properties == null || properties.isEmpty()) {
        return null;
      }
      HashMap<String, Props> props = new HashMap<>();
      for (Pair<String, Props> pair : properties) {
        props.put(pair.getFirst(), pair.getSecond());
      }
      return props;
    } catch (SQLException e) {
      logger.error("Error fetching properties, project id" + projectId + " version " + version, e);
      throw new ProjectManagerException("Error fetching properties", e);
    }
  }

  @Override
  public void cleanOlderProjectVersion(int projectId, int version) throws ProjectManagerException {
    final String DELETE_FLOW = "DELETE FROM project_flows WHERE project_id=? AND version<?";
    final String DELETE_PROPERTIES = "DELETE FROM project_properties WHERE project_id=? AND version<?";
    final String DELETE_PROJECT_FILES = "DELETE FROM project_files WHERE project_id=? AND version<?";
    final String UPDATE_PROJECT_VERSIONS = "UPDATE project_versions SET num_chunks=0 WHERE project_id=? AND version<?";

    SQLTransaction<Integer> cleanOlderProjectTransaction = transOperator -> {
      transOperator.update(DELETE_FLOW, projectId, version);
      transOperator.update(DELETE_PROPERTIES, projectId, version);
      transOperator.update(DELETE_PROJECT_FILES, projectId, version);
      return transOperator.update(UPDATE_PROJECT_VERSIONS, projectId, version);
    };

    try {
      int res = dbOperator.transaction(cleanOlderProjectTransaction);
      if (res == 0) {
        logger.info("clean older project given project id " + projectId + " doesn't take effect.");
      }
    } catch (SQLException e) {
      logger.error("clean older project transaction failed", e);
      throw new ProjectManagerException("clean older project transaction failed", e);
    }
  }
}
