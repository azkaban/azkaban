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

import azkaban.db.EncodingType;
import azkaban.flow.Flow;
import azkaban.user.Permission;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Triple;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.dbutils.ResultSetHandler;


/**
 * This is a JDBC Handler collection place for all project handler classes.
 */
class JdbcProjectHandlerSet {

  public static class ProjectResultHandler implements ResultSetHandler<List<Project>> {

    public static String SELECT_PROJECT_BY_ID =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE id=?";

    public static String SELECT_ALL_ACTIVE_PROJECTS =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE active=true";

    public static String SELECT_ACTIVE_PROJECT_BY_NAME =
        "SELECT id, name, active, modified_time, create_time, version, last_modified_by, description, enc_type, settings_blob FROM projects WHERE name=? AND active=true";

    @Override
    public List<Project> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
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

  public static class ProjectPermissionsResultHandler implements
      ResultSetHandler<List<Triple<String, Boolean, Permission>>> {

    public static String SELECT_PROJECT_PERMISSION =
        "SELECT project_id, modified_time, name, permissions, isGroup FROM project_permissions WHERE project_id=?";

    @Override
    public List<Triple<String, Boolean, Permission>> handle(final ResultSet rs)
        throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final List<Triple<String, Boolean, Permission>> permissions = new ArrayList<>();
      do {
        final String username = rs.getString(3);
        final int permissionFlag = rs.getInt(4);
        final boolean val = rs.getBoolean(5);

        final Permission perm = new Permission(permissionFlag);
        permissions.add(new Triple<>(username, val, perm));
      } while (rs.next());

      return permissions;
    }
  }

  public static class ProjectFlowsResultHandler implements ResultSetHandler<List<Flow>> {

    public static String SELECT_PROJECT_FLOW =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json FROM project_flows WHERE project_id=? AND version=? AND flow_id=?";

    public static String SELECT_ALL_PROJECT_FLOWS =
        "SELECT project_id, version, flow_id, modified_time, encoding_type, json FROM project_flows WHERE project_id=? AND version=?";

    @Override
    public List<Flow> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
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

  public static class ProjectPropertiesResultsHandler implements
      ResultSetHandler<List<Pair<String, Props>>> {

    public static String SELECT_PROJECT_PROPERTY =
        "SELECT project_id, version, name, modified_time, encoding_type, property FROM project_properties WHERE project_id=? AND version=? AND name=?";

    public static String SELECT_PROJECT_PROPERTIES =
        "SELECT project_id, version, name, modified_time, encoding_type, property FROM project_properties WHERE project_id=? AND version=?";

    @Override
    public List<Pair<String, Props>> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final List<Pair<String, Props>> properties = new ArrayList<>();
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

  public static class ProjectLogsResultHandler implements ResultSetHandler<List<ProjectLogEvent>> {

    public static String SELECT_PROJECT_EVENTS_ORDER =
        "SELECT project_id, event_type, event_time, username, message FROM project_events WHERE project_id=? ORDER BY event_time DESC LIMIT ? OFFSET ?";

    @Override
    public List<ProjectLogEvent> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final ArrayList<ProjectLogEvent> events = new ArrayList<>();
      do {
        final int projectId = rs.getInt(1);
        final int eventType = rs.getInt(2);
        final long eventTime = rs.getLong(3);
        final String username = rs.getString(4);
        final String message = rs.getString(5);

        final ProjectLogEvent event =
            new ProjectLogEvent(projectId, ProjectLogEvent.EventType.fromInteger(eventType),
                eventTime, username,
                message);
        events.add(event);
      } while (rs.next());

      return events;
    }
  }

  public static class ProjectFileChunkResultHandler implements ResultSetHandler<List<byte[]>> {

    public static String SELECT_PROJECT_CHUNKS_FILE =
        "SELECT project_id, version, chunk, size, file FROM project_files WHERE project_id=? AND version=? AND chunk >= ? AND chunk < ? ORDER BY chunk ASC";

    @Override
    public List<byte[]> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final ArrayList<byte[]> data = new ArrayList<>();
      do {
        final byte[] bytes = rs.getBytes(5);

        data.add(bytes);
      } while (rs.next());

      return data;
    }
  }

  public static class ProjectVersionResultHandler implements
      ResultSetHandler<List<ProjectFileHandler>> {

    public static String SELECT_PROJECT_VERSION =
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

        final ProjectFileHandler handler =
            new ProjectFileHandler(projectId, version, uploadTime, uploader, fileType, fileName,
                numChunks, md5,
                resourceId);

        handlers.add(handler);
      } while (rs.next());

      return handlers;
    }
  }

  public static class IntHandler implements ResultSetHandler<Integer> {

    public static String SELECT_LATEST_VERSION = "SELECT MAX(version) FROM project_versions WHERE project_id=?";
    public static String SELECT_LATEST_FLOW_VERSION = "SELECT MAX(flow_version) FROM "
        + "project_flow_files WHERE project_id=? AND project_version=? AND flow_name=?";

    @Override
    public Integer handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return 0;
      }

      return rs.getInt(1);
    }
  }

  public static class FlowFileResultHandler implements ResultSetHandler<List<byte[]>> {

    public static String SELECT_FLOW_FILE =
        "SELECT flow_file FROM project_flow_files WHERE "
            + "project_id=? AND project_version=? AND flow_name=? AND flow_version=?";

    public static String SELECT_ALL_FLOW_FILES =
        "SELECT flow_file FROM project_flow_files WHERE "
            + "project_id=? AND project_version=?";

    @Override
    public List<byte[]> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.emptyList();
      }

      final List<byte[]> data = new ArrayList<>();
      do {
        final byte[] bytes = rs.getBytes(1);
        data.add(bytes);
      } while (rs.next());

      return data;
    }
  }
}
