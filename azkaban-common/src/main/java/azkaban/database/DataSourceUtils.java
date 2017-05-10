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

import azkaban.utils.Props;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.log4j.Logger;

public class DataSourceUtils {

  private static Logger logger = Logger.getLogger(DataSourceUtils.class);

  /**
   * Property types
   */
  public static enum PropertyType {
    DB(1);

    private final int numVal;

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
      Path h2DbPath = Paths.get(path).toAbsolutePath();
      logger.info("h2 DB path: " + h2DbPath);
      dataSource = getH2DataSource(h2DbPath);
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
  public static AzkabanDataSource getH2DataSource(Path file) {
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
      setMaxTotal(numConnections);
      setValidationQuery("/* ping */ select 1");
      setTestOnBorrow(true);
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
   *
   */
  public static class EmbeddedH2BasicDataSource extends AzkabanDataSource {
    private EmbeddedH2BasicDataSource(Path filePath) {
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
