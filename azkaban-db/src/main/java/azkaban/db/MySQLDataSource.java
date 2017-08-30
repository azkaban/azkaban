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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.log4j.Logger;

public class MySQLDataSource extends AzkabanDataSource {

  private static final Logger logger = Logger.getLogger(MySQLDataSource.class);
  private DataSource dataSource;

  public MySQLDataSource(final String host, final int port, final String dbName,
      final String user, final String password, final int numConnections) {
    super();

    final String url = "jdbc:mysql://" + (host + ":" + port + "/" + dbName);
    addConnectionProperty("useUnicode", "yes");
    addConnectionProperty("characterEncoding", "UTF-8");
    setDriverClassName("com.mysql.jdbc.Driver");
    setUsername(user);
    setPassword(password);
    setUrl(url);
    setMaxTotal(numConnections);
    setValidationQuery("/* ping */ select 1");
    setTestOnBorrow(true);
  }

  /**
   * This method overrides {@link BasicDataSource#getConnection()}, in order to have retry logics.
   */
  @Override
  public synchronized Connection getConnection() throws SQLException {

    Connection connection = null;
    int retryAttempt = 0;
    while (retryAttempt < AzDBUtil.MAX_DB_RETRY_COUNT) {
      try {
        /**
         * when DB connection could not be fetched (e.g., network issue), or connection can not be validated,
         * {@link BasicDataSource} throws a SQL Exception. {@link BasicDataSource#dataSource} will be reset to null.
         * createDataSource() will create a new dataSource.
         * Every Attempt generates a thread-hanging-time, about 75 seconds, which is hard coded, and can not be changed.
         */
        connection = createDataSource().getConnection();

        /**
         * If connection is null or connection is read only, retry to find available connection.
         * When DB fails over from master to slave, master is set to read-only mode. We must keep
         * finding correct data source and sql connection.
         */
        if (connection == null || isReadOnly(connection)) {
          throw new SQLException("Failed to find DB connection Or connection is read only. ");
        } else {
          return connection;
        }
      } catch (final SQLException ex) {

        logger.error( "Failed to find write-enabled DB connection. Wait 1 minutes and retry."
            + " No.Attempt = " + retryAttempt, ex);
        /**
         * When database is completed down, DB connection fails to be fetched immediately. So we need
         * to hang 1 minute for retry.
         */
        sleep(1000L * 60);
        retryAttempt++;
        this.dataSource = null;
      }
    }
    return connection;
  }

  /**
   * This method keeps the original functionality from parent class. The difference is that we want to
   * manage dataSource in AZ side, so that we can assign null to it and create new dataSource (rather
   * than fetch the existing dataSource) if dataSource is broken but not null.
   */
  @Override
  protected synchronized DataSource createDataSource() throws SQLException {

    if (this.dataSource != null) {
      return this.dataSource;
    }

    // create factory which returns raw physical connections
    final ConnectionFactory driverConnectionFactory = createConnectionFactory();

    // Set up the poolable connection factory
    boolean success = false;
    final PoolableConnectionFactory poolableConnectionFactory;
    try {
      poolableConnectionFactory = createPoolableConnectionFactory(
          driverConnectionFactory);
      poolableConnectionFactory.setPoolStatements(isPoolPreparedStatements());
      poolableConnectionFactory.setMaxOpenPrepatedStatements(getMaxOpenPreparedStatements());
      success = true;
    } catch (final SQLException se) {
      throw se;
    } catch (final RuntimeException rte) {
      throw rte;
    } catch (final Exception ex) {
      throw new SQLException("Error creating connection factory", ex);
    }

    if (success) {
      // create a pool for our connections
      createConnectionPool(poolableConnectionFactory);
    }

    // Create the pooling data source to manage connections
    DataSource newDataSource;
    success = false;
    try {
      newDataSource = createDataSourceInstance();
      success = true;
    } catch (final SQLException se) {
      throw se;
    } catch (final RuntimeException rte) {
      throw rte;
    } catch (final Exception ex) {
      throw new SQLException("Error creating datasource", ex);
    } finally {
      if (!success) {
        // We should also local connection pool field in this class. In this way, we can manage
        // connection pool easily.
        closeConnectionPool(getConnectionPool());
      }
    }

    // If initialSize > 0, preload the pool
    try {
      for (int i = 0; i < getInitialSize(); i++) {
        this.getConnectionPool().addObject();
      }
    } catch (final Exception e) {
      closeConnectionPool(getConnectionPool());
      throw new SQLException("Error preloading the connection pool", e);
    }

    // If timeBetweenEvictionRunsMillis > 0, start the pool's evictor task
    startPoolMaintenance();

    this.dataSource = newDataSource;
    return this.dataSource;
  }

  private void closeConnectionPool(final GenericObjectPool<PoolableConnection> connectionPool) {
  }

  private boolean isReadOnly(final Connection conn) throws SQLException {
    final Statement stmt = conn.createStatement();
    final ResultSet rs = stmt.executeQuery("SELECT @@global.read_only");
    if (rs.next()) {
      final int value = rs.getInt(1);
      return value != 0;
    }
    throw new SQLException("can not fetch read only value from DB");
  }

  private void sleep(final long milliseconds) {
    try {
      Thread.sleep(milliseconds);
    } catch (final InterruptedException e) {
      logger.error("can not sleep", e);
    }
  }

  @Override
  public String getDBType() {
    return "mysql";
  }

  @Override
  public boolean allowsOnDuplicateKey() {
    return true;
  }
}
