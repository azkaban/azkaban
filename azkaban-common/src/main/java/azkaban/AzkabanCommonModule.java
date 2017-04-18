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
import azkaban.db.AzDBOperator;
import azkaban.db.AzDBOperatorImpl;

import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectLoader;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.storage.LocalStorage;
import azkaban.storage.StorageConfig;
import azkaban.storage.StorageImplementationType;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import java.io.File;

import static azkaban.storage.StorageImplementationType.*;


public class AzkabanCommonModule extends AbstractModule {
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
  private final String storageImplementation;

  private final AzkabanDataSource dataSource;

  public AzkabanCommonModule(Props props) {
    this.props = props;
    this.storageImplementation = props.getString(Constants.ConfigurationKeys.AZKABAN_STORAGE_TYPE, LOCAL.name());
    this.dataSource = getDataSource(props);
  }

  @Override
  protected void configure() {
    bind(ProjectLoader.class).to(JdbcProjectLoader.class).in(Scopes.SINGLETON);
    bind(Props.class).toInstance(props);
    bind(Storage.class).to(resolveStorageClassType()).in(Scopes.SINGLETON);
    bind(AzDBOperator.class).to(AzDBOperatorImpl.class).in(Scopes.SINGLETON);
    //todo kunkun-tang : Consider both H2 DataSource and MysqlDatasource case.
    bind(AzkabanDataSource.class).toInstance(dataSource);
  }

  public Class<? extends Storage> resolveStorageClassType() {
    final StorageImplementationType type = StorageImplementationType.from(storageImplementation);
    if (type != null) {
      return type.getImplementationClass();
    } else {
      return loadCustomStorageClass(storageImplementation);
    }
  }

  private Class<? extends Storage> loadCustomStorageClass(String storageImplementation) {
    try {
      return (Class<? extends Storage>) Class.forName(storageImplementation);
    } catch (ClassNotFoundException e) {
      throw new StorageException(e);
    }
  }

  @Inject
  public @Provides
  LocalStorage createLocalStorage(StorageConfig config) {
    return new LocalStorage(new File(config.getBaseDirectoryPath()));
  }

  // todo kunkun-tang: the below method should moved out to azkaban-db module eventually.
  // Today azkaban-db can not rely on Props, so we can not do it.
  private static AzkabanDataSource getDataSource(Props props) {
    String databaseType = props.getString("database.type");

    // todo kunkun-tang: temperaroy workaround to let service provider test work.
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
}
