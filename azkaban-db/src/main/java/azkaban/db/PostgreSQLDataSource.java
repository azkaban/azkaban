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
package azkaban.db;

import azkaban.utils.Props;
import java.sql.Connection;
import java.sql.SQLException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

@Singleton
public class PostgreSQLDataSource extends AzkabanDataSource {

  private static final Logger logger = Logger.getLogger(PostgreSQLDataSource.class);
  public static final String POSTGRES_ER_LOCK_DEADLOCK = "40P01";
  private boolean shutdown;

  private final DBMetrics dbMetrics;

  @Inject
  public PostgreSQLDataSource(final Props props, final DBMetrics dbMetrics) {
    super();
    this.dbMetrics = dbMetrics;

    final int port = props.getInt("postgresql.port");
    final String host = props.getString("postgresql.host");
    final String dbName = props.getString("postgresql.database");
    final String user = props.getString("postgresql.user");
    final String password = props.getString("postgresql.password");
    final int numConnections = props.getInt("postgresql.numconnections");
    final String urlSuffix = props.getString("postgresql.url.suffix");

    final String url = "jdbc:postgresql://" + (host + ":" + port + "/" + dbName + urlSuffix);
    addConnectionProperty("useUnicode", "yes");
    addConnectionProperty("characterEncoding", "UTF-8");
    setDriverClassName("org.postgresql.Driver");
    setUsername(user);
    setPassword(password);
    setUrl(url);
    setMaxTotal(numConnections);
    setValidationQuery("/* ping */ select 1");
    setTestOnBorrow(true);
  }

  @Override
  public synchronized void close() throws SQLException {
    super.close();
    this.shutdown = true;
  }

  /**
   * This method overrides {@link BasicDataSource#getConnection()}, in order to have retry logics.
   * We don't make the call synchronized in order to guarantee normal cases performance.
   */
  @Override
  public Connection getConnection() throws SQLException {

    this.dbMetrics.markDBConnection();
    final long startMs = System.currentTimeMillis();
    Connection connection = null;
    int retryAttempt = 1;
    while (!shutdown && retryAttempt < AzDBUtil.MAX_DB_RETRY_COUNT) {
      try {
        /**
         * when DB connection could not be fetched (e.g., network issue), or connection can not be validated,
         * {@link BasicDataSource} throws a SQL Exception. {@link BasicDataSource#dataSource} will be reset to null.
         * createDataSource() will create a new dataSource.
         * Every Attempt generates a thread-hanging-time, about 75 seconds, which is hard coded, and can not be changed.
         */
        connection = createDataSource().getConnection();

        /**
         * If connection is null, retry to find available connection.
         */
        if (connection == null) {
          throw new SQLException("Failed to find DB connection. ");
        } else {

          // Evalaute how long it takes to get DB Connection.
          this.dbMetrics.setDBConnectionTime(System.currentTimeMillis() - startMs);
          return connection;
        }
      } catch (final SQLException ex) {

        /**
         * invalidate connection and reconstruct it later. if remote IP address is not reachable,
         * it will get hang for a while and throw exception.
         */
        this.dbMetrics.markDBFailConnection();
        try {
          invalidateConnection(connection);
        } catch (final Exception e) {
          logger.error("can not invalidate connection.", e);
        }
        logger.error("Failed to find write-enabled DB connection. Wait 15 seconds and retry."
            + " No.Attempt = " + retryAttempt, ex);
        /**
         * When database is completed down, DB connection fails to be fetched immediately. So we need
         * to sleep 15 seconds for retry.
         */
        sleep(1000L * 15);
        retryAttempt++;
      }
    }
    return connection;
  }

  private void sleep(final long milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (final InterruptedException e) {
      logger.error("Sleep interrupted", e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public String getDBType() {
    return "postgresql";
  }

  @Override
  public boolean allowsOnDuplicateKey() {
    return false;
  }

  @Override
  public boolean allowsOnConflict() {
    return true;
  }

  @Override
  public String getLastInsertIdQuery() {
    return "SELECT LASTVAL()";
  }
}
