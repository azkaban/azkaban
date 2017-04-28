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
 *
 */

package azkaban;

import azkaban.db.AzkabanDataSource;
import azkaban.db.H2FileDataSource;
import azkaban.db.MySQLDataSource;
import azkaban.storage.StorageImplementationType;
import azkaban.utils.Props;
import com.google.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.log4j.Logger;

import static azkaban.Constants.ConfigurationKeys.*;
import static azkaban.storage.StorageImplementationType.*;


public class AzkabanCommonModuleConfig {
  private static final Logger log = Logger.getLogger(AzkabanCommonModuleConfig.class);

  private final Props props;

  /**
   * Storage Implementation
   * This can be any of the {@link StorageImplementationType} values in which case {@link StorageFactory} will create
   * the appropriate storage instance. Or one can feed in a custom implementation class using the full qualified
   * path required by a classloader.
   *
   * examples: LOCAL, DATABASE, azkaban.storage.MyFavStorage
   *
   */
  private String storageImplementation = DATABASE.name();
  private String localStorageBaseDirPath = "AZKABAN_STORAGE";
  private URI hdfsBaseUri = uri("hdfs://localhost:50070/path/to/base/");

  @Inject
  public AzkabanCommonModuleConfig(Props props) {
    this.props = props;

    storageImplementation = props.getString(Constants.ConfigurationKeys.AZKABAN_STORAGE_TYPE,
        storageImplementation);
    localStorageBaseDirPath = props.getString(AZKABAN_STORAGE_LOCAL_BASEDIR, localStorageBaseDirPath);
    hdfsBaseUri = props.getUri(AZKABAN_STORAGE_HDFS_BASEURI, hdfsBaseUri);
  }

  public Props getProps() {
    return props;
  }

  public String getStorageImplementation() {
    return storageImplementation;
  }

  public String getLocalStorageBaseDirPath() {
    return localStorageBaseDirPath;
  }

  // todo kunkun-tang: the below method should moved out to other package.
  public AzkabanDataSource getDataSource() {
    String databaseType = props.getString("database.type");

    if(databaseType.equals("h2")) {
      String path = props.getString("h2.path");
      return new H2FileDataSource(path);
    }
    int port = props.getInt("mysql.port");
    String host = props.getString("mysql.host");
    String database = props.getString("mysql.database");
    String user = props.getString("mysql.user");
    String password = props.getString("mysql.password");
    int numConnections = props.getInt("mysql.numconnections");

    return MySQLDataSource.getInstance(host, port, database, user, password,
        numConnections);

  }

  public URI getHdfsBaseUri() {
    return hdfsBaseUri;
  }

  private static URI uri(String uri){
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      log.error(e);
    }
    return null;
  }
}
