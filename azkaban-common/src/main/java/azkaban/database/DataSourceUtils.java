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

import azkaban.db.AzDBUtil;
import azkaban.utils.Props;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.log4j.Logger;

public class DataSourceUtils {

  private static final Logger logger = Logger.getLogger(DataSourceUtils.class);

  /**
   * Hidden datasource
   */
  private DataSourceUtils() {
  }

  /**
   * Create Datasource from parameters in the properties
   */
  public static AzkabanDataSource getDataSource(final Props props) {
    final String databaseType = props.getString("database.type");

    AzkabanDataSource dataSource = null;
    if (databaseType.equals("mysql")) {
      final int port = props.getInt("mysql.port");
      final String host = props.getString("mysql.host");
      final String database = props.getString("mysql.database");
      final String user = props.getString("mysql.user");
      final String password = props.getString("mysql.password");
      final String driverClassName = props.getString("mysql.driverclassname",
          "com.mysql.cj.jdbc.Driver");
      final int numConnections = props.getInt("mysql.numconnections");

      dataSource =
          getMySQLDataSource(host, port, database, user, password, driverClassName,
              numConnections);
    } else if (databaseType.equals("h2")) {
      final String path = props.getString("h2.path");
      final Path h2DbPath = Paths.get(path).toAbsolutePath();
      logger.info("h2 DB path: " + h2DbPath);
      dataSource = getH2DataSource(h2DbPath);
    }

    return dataSource;
  }

  /**
   * Create a MySQL DataSource
   */
  public static AzkabanDataSource getMySQLDataSource(final String host, final Integer port,
      final String dbName, final String user, final String password,
      String driverClassName, final Integer numConnections) {
    return new MySQLBasicDataSource(host, port, dbName, user, password, driverClassName,
        numConnections);
  }

  /**
   * Create H2 DataSource
   */
  public static AzkabanDataSource getH2DataSource(final Path file) {
    return new EmbeddedH2BasicDataSource(file);
  }

  /**
   * Property types
   */
  public static enum PropertyType {
    DB(1);

    private final int numVal;

    PropertyType(final int numVal) {
      this.numVal = numVal;
    }

    public static PropertyType fromInteger(final int x) {
      switch (x) {
        case 1:
          return DB;
        default:
          return DB;
      }
    }

    public int getNumVal() {
      return this.numVal;
    }
  }

  /**
   * MySQL data source based on AzkabanDataSource
   */
  public static class MySQLBasicDataSource extends AzkabanDataSource {

    private final String url;

    private MySQLBasicDataSource(final String host, final int port, final String dbName,
        final String user, final String password, String driverClassName,
        final int numConnections) {
      super();

      this.url = "jdbc:mysql://" + (host + ":" + port + "/" + dbName);
//      addConnectionProperty("useUnicode", "yes");
//      addConnectionProperty("characterEncoding", "UTF-8");
//      setDriverClassName(driverClassName);
//      setUsername(user);
//      setPassword(password);
//      setUrl(this.url);
//      setMaxTotal(numConnections);
//      setValidationQuery("/* ping */ select 1");
//      setTestOnBorrow(true);
      setJdbcUrl(url);
      setDriverClassName(driverClassName);
      setUsername(user);
      setPassword(password);
      setMinimumIdle(0);
      setMaximumPoolSize(numConnections);
      setConnectionTestQuery("/* ping */ select 1");
      setInitializationFailTimeout(AzDBUtil.MAX_DB_RETRY_COUNT * 1000L * 15);
      addDataSourceProperty("useUnicode", "yes");
      addDataSourceProperty("characterEncoding", "UTF-8");
    }

    @Override
    public boolean allowsOnDuplicateKey() {
      return true;
    }

    @Override
    public String getDBType() {
      return "mysql";
    }
  }

  /**
   * H2 Datasource
   */
  public static class EmbeddedH2BasicDataSource extends AzkabanDataSource {

    private EmbeddedH2BasicDataSource(final Path filePath) {
      super();
      final String url = "jdbc:h2:file:" + filePath + ";IGNORECASE=TRUE";
      setDriverClassName("org.h2.Driver");
      setJdbcUrl(url);
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
