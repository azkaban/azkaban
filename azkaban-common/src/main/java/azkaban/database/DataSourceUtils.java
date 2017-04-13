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


import azkaban.constants.ServerInternals;
import azkaban.utils.Props;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;

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

    private final String url;

    private MySQLBasicDataSource(String host, int port, String dbName,
        String user, String password, int numConnections) {
      super();

      url = "jdbc:mysql://" + (host + ":" + port + "/" + dbName);
      addConnectionProperty("useUnicode", "yes");
      addConnectionProperty("characterEncoding", "UTF-8");
      setDriverClassName("com.mysql.jdbc.Driver");
      setUsername(user);
      setPassword(password);
      setUrl(url);
      setMaxActive(numConnections);
      setValidationQuery("/* ping */ select 1");
      setTestOnBorrow(true);
    }

    /**
     * This method overrides {@link BasicDataSource#getConnection()}, in order to have retry logics.
     *
     */
    @Override
    public Connection getConnection() throws SQLException {

      /*
       * when az server process launches, if the connection isn't valid (e.g., network problem, wrong password, etc),
       * we fail the connection immediately.
       */
      if (getDataSource() == null) {
        return createDataSource().getConnection();
      }

      Connection connection = null;
      int retryAttempt = 0;
      this.dataSource = null;
      while (connection == null && retryAttempt < ServerInternals.MAX_DB_RETRY_COUNT) {
        try {
          /*
           * when DB connection could not be fetched here, dbcp library will keep searching until a timeout defined in
           * its code hardly.
           */
          connection = createDataSource().getConnection();
          if(connection != null)
            return connection;
        } catch (Exception ex) {
          logger.error("Failed to find DB connection", ex);
          this.dataSource = null;
          logger.info("waits 10 seconds and retry. No.Attempt = " + retryAttempt);
//          sleepMillis(1000L);
        } finally {
          retryAttempt ++;
        }
      }
      return connection;
    }

    @Override
    public boolean allowsOnDuplicateKey() {
      return true;
    }

    @Override
    public String getDBType() {
      return "mysql";
    }

    private void sleepMillis(long numMilli) {
      try {
        Thread.sleep(numMilli);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    /*
     * get the parent class's dataSource
     */
    private DataSource getDataSource() {
      return dataSource;
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
}
