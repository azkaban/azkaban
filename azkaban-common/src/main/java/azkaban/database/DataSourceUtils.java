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

package azkaban.database;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.Connection;

import azkaban.utils.Props;

public class DataSourceUtils {

  private static Logger logger = Logger.getLogger(DataSourceUtils.class);

  /**
   * Property types
   */
  public static enum PropertyType {
    DB(1);

    private int numVal;

    PropertyType(int numVal) {
      this.numVal = numVal;
    }

    public int getNumVal() {
      return numVal;
    }

    public static PropertyType fromInteger(int x) {
      switch (x) {
      case 1:
        return DB;
      default:
        return DB;
      }
    }
  }

  /**
   * Create Datasource from parameters in the properties
   *
   * @param props
   * @return
   */
  public static AzkabanDataSource getDataSource(Props props) {
    String databaseType = props.getString("database.type");

    AzkabanDataSource dataSource = null;
    if (databaseType.equals("mysql")) {
      int port = props.getInt("mysql.port");
      String host = props.getString("mysql.host");
      String database = props.getString("mysql.database");
      String user = props.getString("mysql.user");
      String password = props.getString("mysql.password");
      int numConnections = props.getInt("mysql.numconnections");

      dataSource =
          getMySQLDataSource(host, port, database, user, password,
              numConnections);
    } else if (databaseType.equals("h2")) {
      String path = props.getString("h2.path");
      dataSource = getH2DataSource(path);
    }

    return dataSource;
  }

  /**
   * Create a MySQL DataSource
   *
   * @param host
   * @param port
   * @param dbName
   * @param user
   * @param password
   * @param numConnections
   * @return
   */
  public static AzkabanDataSource getMySQLDataSource(String host, Integer port,
      String dbName, String user, String password, Integer numConnections) {
    return new MySQLBasicDataSource(host, port, dbName, user, password,
        numConnections);
  }

  /**
   * Create H2 DataSource
   *
   * @param file
   * @return
   */
  public static AzkabanDataSource getH2DataSource(String file) {
    return new EmbeddedH2BasicDataSource(file);
  }

  /**
   * Hidden datasource
   */
  private DataSourceUtils() {
  }

  /**
   * MySQL data source based on AzkabanDataSource
   *
   */
  public static class MySQLBasicDataSource extends AzkabanDataSource {

    private static MonitorThread monitorThread = null;

    private MySQLBasicDataSource(String host, int port, String dbName,
        String user, String password, int numConnections) {
      super();

      String url = "jdbc:mysql://" + (host + ":" + port + "/" + dbName);
      addConnectionProperty("useUnicode", "yes");
      addConnectionProperty("characterEncoding", "UTF-8");
      setDriverClassName("com.mysql.jdbc.Driver");
      setUsername(user);
      setPassword(password);
      setUrl(url);
      setMaxActive(numConnections);
      setValidationQuery("/* ping */ select 1");
      setTestOnBorrow(true);

      if (monitorThread == null) {
        monitorThread = new MonitorThread(this);
        monitorThread.start();
      }
    }

    @Override
    public boolean allowsOnDuplicateKey() {
      return true;
    }

    @Override
    public String getDBType() {
      return "mysql";
    }

    private class MonitorThread extends Thread {
      private static final long MONITOR_THREAD_WAIT_INTERVAL_MS = 30 * 1000;
      private boolean shutdown = false;
      MySQLBasicDataSource dataSource;

      public MonitorThread(MySQLBasicDataSource mysqlSource) {
        this.setName("MySQL-DB-Monitor-Thread");
        dataSource = mysqlSource;
      }

      @SuppressWarnings("unused")
      public void shutdown() {
        shutdown = true;
        this.interrupt();
      }

      public void run() {
        while (!shutdown) {
          synchronized (this) {
            try {
              pingDB();
              wait(MONITOR_THREAD_WAIT_INTERVAL_MS);
            } catch (InterruptedException e) {
              logger.info("Interrupted. Probably to shut down.");
            }
          }
        }
      }

      private void pingDB() {
        Connection connection = null;
        try {
          connection = dataSource.getConnection();
          PreparedStatement query = connection.prepareStatement("SELECT 1");
          query.execute();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          logger
              .error("MySQL connection test failed. Please check MySQL connection health!");
        } finally {
          DbUtils.closeQuietly(connection);
        }
      }
    }

  }

  /**
   * H2 Datasource
   *
   */
  public static class EmbeddedH2BasicDataSource extends AzkabanDataSource {
    private EmbeddedH2BasicDataSource(String filePath) {
      super();
      String url = "jdbc:h2:file:" + filePath;
      setDriverClassName("org.h2.Driver");
      setUrl(url);
    }

    @Override
    public boolean allowsOnDuplicateKey() {
      return false;
    }

    @Override
    public String getDBType() {
      return "h2";
    }
  }

  public static void testConnection(DataSource ds) throws SQLException {
    QueryRunner runner = new QueryRunner(ds);
    runner.update("SHOW TABLES");
  }

  public static void testMySQLConnection(String host, Integer port,
      String dbName, String user, String password, Integer numConnections)
      throws SQLException {
    DataSource ds =
        new MySQLBasicDataSource(host, port, dbName, user, password,
            numConnections);
    testConnection(ds);
  }
}
