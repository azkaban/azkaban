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

package azkaban.trigger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;

import org.joda.time.DateTime;

import azkaban.database.AbstractJdbcLoader;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import azkaban.utils.Props;

public class JdbcTriggerLoader extends AbstractJdbcLoader implements
    TriggerLoader {
  private static Logger logger = Logger.getLogger(JdbcTriggerLoader.class);

  private EncodingType defaultEncodingType = EncodingType.GZIP;

  private static final String triggerTblName = "triggers";

  private static final String GET_UPDATED_TRIGGERS =
      "SELECT trigger_id, trigger_source, modify_time, enc_type, data FROM "
          + triggerTblName + " WHERE modify_time>=?";

  private static String GET_ALL_TRIGGERS =
      "SELECT trigger_id, trigger_source, modify_time, enc_type, data FROM "
          + triggerTblName;

  private static String GET_TRIGGER =
      "SELECT trigger_id, trigger_source, modify_time, enc_type, data FROM "
          + triggerTblName + " WHERE trigger_id=?";

  private static String ADD_TRIGGER = "INSERT INTO " + triggerTblName
      + " ( modify_time) values (?)";

  private static String REMOVE_TRIGGER = "DELETE FROM " + triggerTblName
      + " WHERE trigger_id=?";

  private static String UPDATE_TRIGGER =
      "UPDATE "
          + triggerTblName
          + " SET trigger_source=?, modify_time=?, enc_type=?, data=? WHERE trigger_id=?";

  public EncodingType getDefaultEncodingType() {
    return defaultEncodingType;
  }

  public void setDefaultEncodingType(EncodingType defaultEncodingType) {
    this.defaultEncodingType = defaultEncodingType;
  }

  public JdbcTriggerLoader(Props props) {
    super(props);
  }

  @Override
  public List<Trigger> getUpdatedTriggers(long lastUpdateTime)
      throws TriggerLoaderException {
    logger.info("Loading triggers changed since "
        + new DateTime(lastUpdateTime).toString());
    Connection connection = getConnection();

    QueryRunner runner = new QueryRunner();
    ResultSetHandler<List<Trigger>> handler = new TriggerResultHandler();

    List<Trigger> triggers;

    try {
      triggers =
          runner.query(connection, GET_UPDATED_TRIGGERS, handler,
              lastUpdateTime);
    } catch (SQLException e) {
      logger.error(GET_ALL_TRIGGERS + " failed.");

      throw new TriggerLoaderException("Loading triggers from db failed. ", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    logger.info("Loaded " + triggers.size() + " triggers.");

    return triggers;
  }

  @Override
  public List<Trigger> loadTriggers() throws TriggerLoaderException {
    logger.info("Loading all triggers from db.");
    Connection connection = getConnection();

    QueryRunner runner = new QueryRunner();
    ResultSetHandler<List<Trigger>> handler = new TriggerResultHandler();

    List<Trigger> triggers;

    try {
      triggers = runner.query(connection, GET_ALL_TRIGGERS, handler);
    } catch (SQLException e) {
      logger.error(GET_ALL_TRIGGERS + " failed.");

      throw new TriggerLoaderException("Loading triggers from db failed. ", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    logger.info("Loaded " + triggers.size() + " triggers.");

    return triggers;
  }

  @Override
  public void removeTrigger(Trigger t) throws TriggerLoaderException {
    logger.info("Removing trigger " + t.toString() + " from db.");

    QueryRunner runner = createQueryRunner();
    try {
      int removes = runner.update(REMOVE_TRIGGER, t.getTriggerId());
      if (removes == 0) {
        throw new TriggerLoaderException("No trigger has been removed.");
      }
    } catch (SQLException e) {
      logger.error(REMOVE_TRIGGER + " failed.");
      throw new TriggerLoaderException("Remove trigger " + t.toString()
          + " from db failed. ", e);
    }
  }

  @Override
  public void addTrigger(Trigger t) throws TriggerLoaderException {
    logger.info("Inserting trigger " + t.toString() + " into db.");
    t.setLastModifyTime(System.currentTimeMillis());
    Connection connection = getConnection();
    try {
      addTrigger(connection, t, defaultEncodingType);
    } catch (Exception e) {
      throw new TriggerLoaderException("Error uploading trigger", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private synchronized void addTrigger(Connection connection, Trigger t,
      EncodingType encType) throws TriggerLoaderException {

    QueryRunner runner = new QueryRunner();

    long id;

    try {
      runner.update(connection, ADD_TRIGGER, DateTime.now().getMillis());
      connection.commit();
      id =
          runner.query(connection, LastInsertID.LAST_INSERT_ID,
              new LastInsertID());

      if (id == -1L) {
        logger.error("trigger id is not properly created.");
        throw new TriggerLoaderException("trigger id is not properly created.");
      }

      t.setTriggerId((int) id);
      updateTrigger(t);
      logger.info("uploaded trigger " + t.getDescription());
    } catch (SQLException e) {
      throw new TriggerLoaderException("Error creating trigger.", e);
    }

  }

  @Override
  public void updateTrigger(Trigger t) throws TriggerLoaderException {
    if (logger.isDebugEnabled()) {
      logger.debug("Updating trigger " + t.getTriggerId() + " into db.");
    }
    t.setLastModifyTime(System.currentTimeMillis());
    Connection connection = getConnection();
    try {
      updateTrigger(connection, t, defaultEncodingType);
    } catch (Exception e) {
      e.printStackTrace();
      throw new TriggerLoaderException("Failed to update trigger "
          + t.toString() + " into db!");
    } finally {
      DbUtils.closeQuietly(connection);
    }
  }

  private void updateTrigger(Connection connection, Trigger t,
      EncodingType encType) throws TriggerLoaderException {

    String json = JSONUtils.toJSON(t.toJson());
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
      throw new TriggerLoaderException("Error encoding the trigger "
          + t.toString());
    }

    QueryRunner runner = new QueryRunner();

    try {
      int updates =
          runner.update(connection, UPDATE_TRIGGER, t.getSource(),
              t.getLastModifyTime(), encType.getNumVal(), data,
              t.getTriggerId());
      connection.commit();
      if (updates == 0) {
        throw new TriggerLoaderException("No trigger has been updated.");
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Updated " + updates + " records.");
        }
      }
    } catch (SQLException e) {
      logger.error(UPDATE_TRIGGER + " failed.");
      throw new TriggerLoaderException("Update trigger " + t.toString()
          + " into db failed. ", e);
    }
  }

  private static class LastInsertID implements ResultSetHandler<Long> {
    private static String LAST_INSERT_ID = "SELECT LAST_INSERT_ID()";

    @Override
    public Long handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return -1L;
      }

      long id = rs.getLong(1);
      return id;
    }

  }

  public class TriggerResultHandler implements ResultSetHandler<List<Trigger>> {

    @Override
    public List<Trigger> handle(ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Trigger> emptyList();
      }

      ArrayList<Trigger> triggers = new ArrayList<Trigger>();
      do {
        int triggerId = rs.getInt(1);
        int encodingType = rs.getInt(4);
        byte[] data = rs.getBytes(5);

        Object jsonObj = null;
        if (data != null) {
          EncodingType encType = EncodingType.fromInteger(encodingType);

          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            if (encType == EncodingType.GZIP) {
              // Decompress the sucker.
              String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
              jsonObj = JSONUtils.parseJSONFromString(jsonString);
            } else {
              String jsonString = new String(data, "UTF-8");
              jsonObj = JSONUtils.parseJSONFromString(jsonString);
            }
          } catch (IOException e) {
            throw new SQLException("Error reconstructing trigger data ");
          }
        }

        Trigger t = null;
        try {
          t = Trigger.fromJson(jsonObj);
          triggers.add(t);
        } catch (Exception e) {
          e.printStackTrace();
          logger.error("Failed to load trigger " + triggerId);
        }
      } while (rs.next());

      return triggers;
    }

  }

  private Connection getConnection() throws TriggerLoaderException {
    Connection connection = null;
    try {
      connection = super.getDBConnection(false);
    } catch (Exception e) {
      DbUtils.closeQuietly(connection);
      throw new TriggerLoaderException("Error getting DB connection.", e);
    }

    return connection;
  }

  @Override
  public Trigger loadTrigger(int triggerId) throws TriggerLoaderException {
    logger.info("Loading trigger " + triggerId + " from db.");
    Connection connection = getConnection();

    QueryRunner runner = new QueryRunner();
    ResultSetHandler<List<Trigger>> handler = new TriggerResultHandler();

    List<Trigger> triggers;

    try {
      triggers = runner.query(connection, GET_TRIGGER, handler, triggerId);
    } catch (SQLException e) {
      logger.error(GET_TRIGGER + " failed.");
      throw new TriggerLoaderException("Loading trigger from db failed. ", e);
    } finally {
      DbUtils.closeQuietly(connection);
    }

    if (triggers.size() == 0) {
      logger.error("Loaded 0 triggers. Failed to load trigger " + triggerId);
      throw new TriggerLoaderException(
          "Loaded 0 triggers. Failed to load trigger " + triggerId);
    }

    return triggers.get(0);
  }

}
