/*
 * Copyright 2012 LinkedIn, Inc
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

package azkaban.migration.scheduler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

import org.joda.time.DateTimeZone;
import org.joda.time.ReadablePeriod;

import azkaban.database.DataSourceUtils;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

@Deprecated
public class JdbcScheduleLoader implements ScheduleLoader {

  private static Logger logger = Logger.getLogger(JdbcScheduleLoader.class);

  public static enum EncodingType {
    PLAIN(1), GZIP(2);

    private int numVal;

    EncodingType(int numVal) {
      this.numVal = numVal;
    }

    public int getNumVal() {
      return numVal;
    }

    public static EncodingType fromInteger(int x) {
      switch (x) {
      case 1:
        return PLAIN;
      case 2:
        return GZIP;
      default:
        return PLAIN;
      }
    }
  }

  private DataSource dataSource;
  private EncodingType defaultEncodingType = EncodingType.GZIP;

  private static final String scheduleTableName = "schedules";

  private static String SELECT_ALL_SCHEDULES =
      "SELECT project_id, project_name, flow_name, status, "
          + "first_sched_time, timezone, period, last_modify_time, "
          + "next_exec_time, submit_time, submit_user, enc_type, "
          + "schedule_options FROM "
          + scheduleTableName;

  private static String INSERT_SCHEDULE =
      "INSERT INTO "
          + scheduleTableName
          + " ( project_id, project_name, flow_name, status, "
          + "first_sched_time, timezone, period, last_modify_time, "
          + "next_exec_time, submit_time, submit_user, enc_type, "
          + "schedule_options) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";

  private static String REMOVE_SCHEDULE_BY_KEY = "DELETE FROM "
      + scheduleTableName + " WHERE project_id=? AND flow_name=?";

  private static String UPDATE_SCHEDULE_BY_KEY =
      "UPDATE "
          + scheduleTableName
          + " SET status=?, first_sched_time=?, timezone=?, period=?, "
          + "last_modify_time=?, next_exec_time=?, submit_time=?, "
          + "submit_user=?, enc_type=?, schedule_options=? "
          + "WHERE project_id=? AND flow_name=?";

  private static String UPDATE_NEXT_EXEC_TIME = "UPDATE " + scheduleTableName
      + " SET next_exec_time=? WHERE project_id=? AND flow_name=?";

  private Connection getConnection() throws ScheduleManagerException {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (Exception e) {
      DbUtils.closeQuietly(connection);
      throw new ScheduleManagerException("Error getting DB connection.", e);
    }

    return connection;
  }

  public EncodingType getDefaultEncodingType() {
    return defaultEncodingType;
  }

  public void setDefaultEncodingType(EncodingType defaultEncodingType) {
    this.defaultEncodingType = defaultEncodingType;
  }

  public JdbcScheduleLoader(Props props) {
    String databaseType = props.getString("database.type");

    if (databaseType.equals("mysql")) {
      int port = props.getInt("mysql.port");
      String host = props.getString("mysql.host");
      String database = props.getString("mysql.database");
      String user = props.getString("mysql.user");
      String password = props.getString("mysql.password");
      int numConnections = props.getInt("mysql.numconnections");

      dataSource =
          DataSourceUtils.getMySQLDataSource(host, port, database, user,
              password, numConnections);
    }
  }

  @Override
  public List<Schedule> loadSchedules() throws ScheduleManagerException {
    logger.info("Loading all schedules from db.");
    Connection connection = getConnection();

    QueryRunner runner = new QueryRunner();
    ResultSetHandler<List<Schedule>> handler = new ScheduleResultHandler();

    List<Schedule> schedules;

    try {
      schedules = runner.query(connection, SELECT_ALL_SCHEDULES, handler);
    } catch (SQLException e) {
      logger.error(SELECT_ALL_SCHEDULES + " failed.");

      DbUtils.closeQuietly(connection);
      throw new ScheduleManagerException("Loading schedules from db failed. ",
          e);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    logger.info("Now trying to update the schedules");

    // filter the schedules
    Iterator<Schedule> scheduleIterator = schedules.iterator();
    while (scheduleIterator.hasNext()) {
      Schedule sched = scheduleIterator.next();
      if (!sched.updateTime()) {
        logger.info("Schedule " + sched.getScheduleName()
            + " was scheduled before azkaban start, skipping it.");
        scheduleIterator.remove();
        removeSchedule(sched);
      } else {
        logger.info("Recurring schedule, need to update next exec time");
        try {
          updateNextExecTime(sched);
        } catch (Exception e) {
          e.printStackTrace();
          throw new ScheduleManagerException(
              "Update next execution time failed.", e);
        }
        logger.info("Schedule " + sched.getScheduleName()
            + " loaded and updated.");
      }
    }

    logger.info("Loaded " + schedules.size() + " schedules.");

    return schedules;
  }

  @Override
  public void removeSchedule(Schedule s) throws ScheduleManagerException {
    logger.info("Removing schedule " + s.getScheduleName() + " from db.");

    QueryRunner runner = new QueryRunner(dataSource);

    try {
      int removes =
          runner.update(REMOVE_SCHEDULE_BY_KEY, s.getProjectId(),
              s.getFlowName());
      if (removes == 0) {
        throw new ScheduleManagerException("No schedule has been removed.");
      }
    } catch (SQLException e) {
      logger.error(REMOVE_SCHEDULE_BY_KEY + " failed.");
      throw new ScheduleManagerException("Remove schedule "
          + s.getScheduleName() + " from db failed. ", e);
    }
  }

  public void insertSchedule(Schedule s) throws ScheduleManagerException {
    logger.info("Inserting schedule " + s.getScheduleName() + " into db.");
    insertSchedule(s, defaultEncodingType);
  }

  public void insertSchedule(Schedule s, EncodingType encType)
      throws ScheduleManagerException {

    String json = JSONUtils.toJSON(s.optionsToObject());
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
      throw new ScheduleManagerException(
          "Error encoding the schedule options. " + s.getScheduleName());
    }

    QueryRunner runner = new QueryRunner(dataSource);
    try {
      int inserts =
          runner.update(INSERT_SCHEDULE, s.getProjectId(), s.getProjectName(),
              s.getFlowName(), s.getStatus(), s.getFirstSchedTime(), s
                  .getTimezone().getID(), Schedule.createPeriodString(s
                  .getPeriod()), s.getLastModifyTime(), s.getNextExecTime(), s
                  .getSubmitTime(), s.getSubmitUser(), encType.getNumVal(),
              data);
      if (inserts == 0) {
        throw new ScheduleManagerException("No schedule has been inserted.");
      }
    } catch (SQLException e) {
      logger.error(INSERT_SCHEDULE + " failed.");
      throw new ScheduleManagerException("Insert schedule "
          + s.getScheduleName() + " into db failed. ", e);
    }
  }

  @Override
  public void updateNextExecTime(Schedule s) throws ScheduleManagerException {
    logger.info("Update schedule " + s.getScheduleName() + " into db. ");
    Connection connection = getConnection();
    QueryRunner runner = new QueryRunner();
    try {

      runner.update(connection, UPDATE_NEXT_EXEC_TIME, s.getNextExecTime(),
          s.getProjectId(), s.getFlowName());
    } catch (SQLException e) {
      e.printStackTrace();
      logger.error(UPDATE_NEXT_EXEC_TIME + " failed.", e);
      throw new ScheduleManagerException("Update schedule "
          + s.getScheduleName() + " into db failed. ", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  @Override
  public void updateSchedule(Schedule s) throws ScheduleManagerException {
    logger.info("Updating schedule " + s.getScheduleName() + " into db.");
    updateSchedule(s, defaultEncodingType);
  }

  public void updateSchedule(Schedule s, EncodingType encType)
      throws ScheduleManagerException {

    String json = JSONUtils.toJSON(s.optionsToObject());
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
      throw new ScheduleManagerException("Error encoding the schedule options "
          + s.getScheduleName());
    }

    QueryRunner runner = new QueryRunner(dataSource);

    try {
      int updates =
          runner.update(UPDATE_SCHEDULE_BY_KEY, s.getStatus(),
              s.getFirstSchedTime(), s.getTimezone().getID(),
              Schedule.createPeriodString(s.getPeriod()),
              s.getLastModifyTime(), s.getNextExecTime(), s.getSubmitTime(),
              s.getSubmitUser(), encType.getNumVal(), data, s.getProjectId(),
              s.getFlowName());
      if (updates == 0) {
        throw new ScheduleManagerException("No schedule has been updated.");
      }
    } catch (SQLException e) {
      logger.error(UPDATE_SCHEDULE_BY_KEY + " failed.");
      throw new ScheduleManagerException("Update schedule "
          + s.getScheduleName() + " into db failed. ", e);
    }
  }

  public class ScheduleResultHandler implements
      ResultSetHandler<List<Schedule>> {
    @Override
    public List<Schedule> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Schedule> emptyList();
      }

      ArrayList<Schedule> schedules = new ArrayList<Schedule>();
      do {
        int projectId = rs.getInt(1);
        String projectName = rs.getString(2);
        String flowName = rs.getString(3);
        String status = rs.getString(4);
        long firstSchedTime = rs.getLong(5);
        DateTimeZone timezone = DateTimeZone.forID(rs.getString(6));
        ReadablePeriod period = Schedule.parsePeriodString(rs.getString(7));
        long lastModifyTime = rs.getLong(8);
        long nextExecTime = rs.getLong(9);
        long submitTime = rs.getLong(10);
        String submitUser = rs.getString(11);
        int encodingType = rs.getInt(12);
        byte[] data = rs.getBytes(13);

        Object optsObj = null;
        if (data != null) {
          EncodingType encType = EncodingType.fromInteger(encodingType);

          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              optsObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              String jsonString = new String(data, "UTF-8");
              optsObj = JSONUtils.parseJSONFromString(jsonString);
            }
          } catch (IOException e) {
            throw new SQLException("Error reconstructing schedule options "
                + projectName + "." + flowName);
          }
        }

        Schedule s =
            new Schedule(projectId, projectName, flowName, status,
                firstSchedTime, timezone, period, lastModifyTime, nextExecTime,
                submitTime, submitUser);
        if (optsObj != null) {
          s.createAndSetScheduleOptions(optsObj);
        }

        schedules.add(s);
      } while (rs.next());

      return schedules;
    }

  }
}
