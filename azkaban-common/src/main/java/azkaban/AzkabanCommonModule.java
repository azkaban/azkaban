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
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseOperatorImpl;
import azkaban.db.H2FileDataSource;
import azkaban.db.MySQLDataSource;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManager;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectLoader;
import azkaban.spi.AzkabanException;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.storage.StorageImplementationType;
import azkaban.trigger.JdbcTriggerImpl;
import azkaban.trigger.TriggerLoader;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.Constants.ConfigurationKeys.*;
import static com.google.common.base.Preconditions.*;
import static java.util.Objects.*;


/**
 * This Guice module is currently a one place container for all bindings in the current module. This is intended to
 * help during the migration process to Guice. Once this class starts growing we can move towards more modular
 * structuring of Guice components.
 */
public class AzkabanCommonModule extends AbstractModule {
  private static final Logger log = LoggerFactory.getLogger(AzkabanCommonModule.class);

  private final Props props;
  private final AzkabanCommonModuleConfig config;

  public AzkabanCommonModule(Props props) {
    this.props = props;
    this.config = new AzkabanCommonModuleConfig(props);
  }

  @Override
  protected void configure() {
    bind(ExecutorLoader.class).to(JdbcExecutorLoader.class).in(Scopes.SINGLETON);
    bind(ProjectLoader.class).to(JdbcProjectLoader.class).in(Scopes.SINGLETON);
    bind(Props.class).toInstance(config.getProps());
    bind(Storage.class).to(resolveStorageClassType()).in(Scopes.SINGLETON);
    bind(DatabaseOperator.class).to(DatabaseOperatorImpl.class).in(Scopes.SINGLETON);
    bind(TriggerLoader.class).to(JdbcTriggerImpl.class).in(Scopes.SINGLETON);
    bind(DataSource.class).to(AzkabanDataSource.class);
    bind(ExecutorManager.class).in(Scopes.SINGLETON);
    bind(AlerterHolder.class).in(Scopes.SINGLETON);
  }

  public Class<? extends Storage> resolveStorageClassType() {
    final StorageImplementationType type = StorageImplementationType.from(config.getStorageImplementation());
    if (type != null) {
      return type.getImplementationClass();
    } else {
      return loadCustomStorageClass(config.getStorageImplementation());
    }
  }

  @SuppressWarnings("unchecked")
  private Class<? extends Storage> loadCustomStorageClass(String storageImplementation) {
    try {
      return (Class<? extends Storage>) Class.forName(storageImplementation);
    } catch (ClassNotFoundException e) {
      throw new StorageException(e);
    }
  }

  // todo kunkun-tang: the below method should moved out to azkaban-db module eventually.
  @Inject
  @Provides
  @Singleton
  public AzkabanDataSource getDataSource(Props props) {
    String databaseType = props.getString("database.type");

    if(databaseType.equals("h2")) {
      String path = props.getString("h2.path");
      Path h2DbPath = Paths.get(path).toAbsolutePath();
      log.info("h2 DB path: " + h2DbPath);
      return new H2FileDataSource(h2DbPath);
    }
    int port = props.getInt("mysql.port");
    String host = props.getString("mysql.host");
    String database = props.getString("mysql.database");
    String user = props.getString("mysql.user");
    String password = props.getString("mysql.password");
    int numConnections = props.getInt("mysql.numconnections");

    return MySQLDataSource.getInstance(host, port, database, user, password, numConnections);
  }

  @Inject
  @Provides
  @Singleton
  public Configuration createHadoopConfiguration() {
    final String hadoopConfDirPath = requireNonNull(props.get(HADOOP_CONF_DIR_PATH));

    final File hadoopConfDir = new File(requireNonNull(hadoopConfDirPath));
    checkArgument(hadoopConfDir.exists() && hadoopConfDir.isDirectory());

    final Configuration hadoopConf = new Configuration(false);
    hadoopConf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDirPath, "core-site.xml"));
    hadoopConf.addResource(new org.apache.hadoop.fs.Path(hadoopConfDirPath, "hdfs-site.xml"));
    hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    return hadoopConf;
  }

  @Inject
  @Provides
  @Singleton
  public FileSystem createHadoopFileSystem(final Configuration hadoopConf) {
    try {
      return FileSystem.get(hadoopConf);
    } catch (IOException e) {
      log.error("Unable to initialize HDFS", e);
      throw new AzkabanException(e);
    }
  }

  @Provides
  public QueryRunner createQueryRunner(AzkabanDataSource dataSource) {
    return new QueryRunner(dataSource);
  }
}
