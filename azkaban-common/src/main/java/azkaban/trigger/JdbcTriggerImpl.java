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

package azkaban.trigger;

import azkaban.db.EncodingType;
import azkaban.db.DatabaseOperator;
import azkaban.db.SQLTransaction;
import azkaban.utils.GZIPUtils;
import azkaban.utils.JSONUtils;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;


@Singleton
public class JdbcTriggerImpl implements TriggerLoader {

  private static final String TRIGGER_TABLE_NAME = "triggers";
  private static final String GET_UPDATED_TRIGGERS =
      "SELECT trigger_id, trigger_source, modify_time, enc_type, data FROM " + TRIGGER_TABLE_NAME
          + " WHERE modify_time>=?";
  private static final String GET_ALL_TRIGGERS =
      "SELECT trigger_id, trigger_source, modify_time, enc_type, data FROM " + TRIGGER_TABLE_NAME;
  private static final String GET_TRIGGER =
      "SELECT trigger_id, trigger_source, modify_time, enc_type, data FROM " + TRIGGER_TABLE_NAME
          + " WHERE trigger_id=?";
  private static final String ADD_TRIGGER =
      "INSERT INTO " + TRIGGER_TABLE_NAME + " ( modify_time) values (?)";
  private static final String REMOVE_TRIGGER =
      "DELETE FROM " + TRIGGER_TABLE_NAME + " WHERE trigger_id=?";
  private static final String UPDATE_TRIGGER =
      "UPDATE " + TRIGGER_TABLE_NAME
          + " SET trigger_source=?, modify_time=?, enc_type=?, data=? WHERE trigger_id=?";
  private static final Logger logger = Logger.getLogger(JdbcTriggerImpl.class);
  private final DatabaseOperator dbOperator;
  private final EncodingType defaultEncodingType = EncodingType.GZIP;

  @Inject
  public JdbcTriggerImpl(final DatabaseOperator databaseOperator) {
    this.dbOperator = databaseOperator;
  }

  @Override
  public List<Trigger> getUpdatedTriggers(final long lastUpdateTime) throws TriggerLoaderException {
    logger.info("Loading triggers changed since " + new DateTime(lastUpdateTime).toString());

    final ResultSetHandler<List<Trigger>> handler = new TriggerResultHandler();

    try {
      final List<Trigger> triggers = this.dbOperator
          .query(GET_UPDATED_TRIGGERS, handler, lastUpdateTime);
      logger.info("Loaded " + triggers.size() + " triggers.");
      return triggers;
    } catch (final SQLException ex) {
      throw new TriggerLoaderException("Loading triggers from db failed.", ex);
    }
  }

  @Override
  public List<Trigger> loadTriggers() throws TriggerLoaderException {
    logger.info("Loading all triggers from db.");

    final ResultSetHandler<List<Trigger>> handler = new TriggerResultHandler();

    try {
      final List<Trigger> triggers = this.dbOperator.query(GET_ALL_TRIGGERS, handler);
      logger.info("Loaded " + triggers.size() + " triggers.");
      return triggers;
    } catch (final SQLException ex) {
      throw new TriggerLoaderException("Loading triggers from db failed.", ex);
    }
  }

  @Override
  public void removeTrigger(final Trigger t) throws TriggerLoaderException {
    logger.info("Removing trigger " + t.toString() + " from db.");

    try {
      final int removes = this.dbOperator.update(REMOVE_TRIGGER, t.getTriggerId());
      if (removes == 0) {
        throw new TriggerLoaderException("No trigger has been removed.");
      }
    } catch (final SQLException ex) {
      throw new TriggerLoaderException("Remove trigger " + t.getTriggerId() + " from db failed. ",
          ex);
    }
  }

  @Override
  public void addTrigger(final Trigger t) throws TriggerLoaderException {
    logger.info("Inserting trigger " + t.toString() + " into db.");

    final SQLTransaction<Long> insertAndGetLastID = transOperator -> {
      transOperator.update(ADD_TRIGGER, DateTime.now().getMillis());
      // This commit must be called in order to unlock trigger table and have last insert ID.
      transOperator.getConnection().commit();
      return transOperator.getLastInsertId();
    };

    try {
      final long id = this.dbOperator.transaction(insertAndGetLastID);
      t.setTriggerId((int) id);
      updateTrigger(t);
      logger.info("uploaded trigger " + t.getDescription());
    } catch (final SQLException ex) {
      logger.error("Adding Trigger " + t.getTriggerId() + " failed.");
      throw new TriggerLoaderException("trigger id is not properly created.", ex);
    }
  }

  @Override
  public void updateTrigger(final Trigger t) throws TriggerLoaderException {
    logger.info("Updating trigger " + t.getTriggerId() + " into db.");
    t.setLastModifyTime(System.currentTimeMillis());
    updateTrigger(t, this.defaultEncodingType);
  }

  private void updateTrigger(final Trigger t, final EncodingType encType)
      throws TriggerLoaderException {

    final String json = JSONUtils.toJSON(t.toJson());
    byte[] data = null;
    try {
      final byte[] stringData = json.getBytes("UTF-8");
      data = stringData;

      if (encType == EncodingType.GZIP) {
        data = GZIPUtils.gzipBytes(stringData);
      }
      logger.debug(
          "NumChars: " + json.length() + " UTF-8:" + stringData.length + " Gzip:" + data.length);
    } catch (final IOException e) {
      logger.error("Trigger encoding fails", e);
      throw new TriggerLoaderException("Error encoding the trigger " + t.toString(), e);
    }

    try {
      final int updates = this.dbOperator
          .update(UPDATE_TRIGGER, t.getSource(), t.getLastModifyTime(), encType.getNumVal(), data,
              t.getTriggerId());
      if (updates == 0) {
        throw new TriggerLoaderException("No trigger has been updated.");
      }
    } catch (final SQLException ex) {
      logger.error("Updating Trigger " + t.getTriggerId() + " failed.");
      throw new TriggerLoaderException("DB Trigger update failed. ", ex);
    }
  }

  @Override
  public Trigger loadTrigger(final int triggerId) throws TriggerLoaderException {
    logger.info("Loading trigger " + triggerId + " from db.");
    final ResultSetHandler<List<Trigger>> handler = new TriggerResultHandler();

    try {
      final List<Trigger> triggers = this.dbOperator.query(GET_TRIGGER, handler, triggerId);

      if (triggers.size() == 0) {
        logger.error("Loaded 0 triggers. Failed to load trigger " + triggerId);
        throw new TriggerLoaderException("Loaded 0 triggers. Failed to load trigger " + triggerId);
      }
      return triggers.get(0);
    } catch (final SQLException ex) {
      logger.error("Failed to load trigger " + triggerId);
      throw new TriggerLoaderException("Load a specific trigger failed.", ex);
    }
  }

  public static class TriggerResultHandler implements ResultSetHandler<List<Trigger>> {

    @Override
    public List<Trigger> handle(final ResultSet rs) throws SQLException {
      if (!rs.next()) {
        return Collections.<Trigger>emptyList();
      }

      final ArrayList<Trigger> triggers = new ArrayList<>();
      do {
        final int triggerId = rs.getInt(1);
        final int encodingType = rs.getInt(4);
        final byte[] data = rs.getBytes(5);

        Object jsonObj = null;
        if (data != null) {
          final EncodingType encType = EncodingType.fromInteger(encodingType);

          try {
            // Convoluted way to inflate strings. Should find common package or
            // helper function.
            jsonObj = JSONUtils.parseJSONFromString(encType == EncodingType.GZIP ?
                GZIPUtils.unGzipString(data, "UTF-8") : new String(data, "UTF-8"));
          } catch (final IOException e) {
            throw new SQLException("Error reconstructing trigger data ");
          }
        }

        Trigger t = null;
        try {
          t = Trigger.fromJson(jsonObj);
          // if detecting any miss schedules, send task to missScheduleManager
          t.sendTaskToMissedScheduleManager();
          triggers.add(t);
        } catch (final Exception e) {
          logger.error("Failed to load trigger " + triggerId, e);
        }
      } while (rs.next());

      return triggers;
    }
  }
}
