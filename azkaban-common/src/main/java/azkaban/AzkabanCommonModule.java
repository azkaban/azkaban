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

import static azkaban.Constants.ConfigurationKeys.HADOOP_CONF_DIR_PATH;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import azkaban.db.AzkabanDataSource;
import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseOperatorImpl;
import azkaban.db.H2FileDataSource;
import azkaban.db.MySQLDataSource;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.ProjectLoader;
import azkaban.spi.AzkabanException;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.storage.HdfsAuth;
import azkaban.storage.StorageImplementationType;
import azkaban.trigger.JdbcTriggerImpl;
import azkaban.trigger.TriggerLoader;
import azkaban.utils.Props;
import com.codahale.metrics.MetricRegistry;
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


/**
 * This Guice module is currently a one place container for all bindings in the current module. This
 * is intended to help during the migration process to Guice. Once this class starts growing we can
 * move towards more modular structuring of Guice components.
 */
public class AzkabanCommonModule extends AbstractModule {

  private static final Logger log = LoggerFactory.getLogger(AzkabanCommonModule.class);

  private final Props props;
  private final AzkabanCommonModuleConfig config;

  public AzkabanCommonModule(final Props props) {
    this.props = props;
    this.config = new AzkabanCommonModuleConfig(props);
  }

  @Override
  protected void configure() {
    bind(Props.class).toInstance(this.config.getProps());
    bind(Storage.class).to(resolveStorageClassType());
    bind(DatabaseOperator.class).to(DatabaseOperatorImpl.class);
    bind(TriggerLoader.class).to(JdbcTriggerImpl.class);
    bind(ProjectLoader.class).to(JdbcProjectImpl.class);
    bind(DataSource.class).to(AzkabanDataSource.class);
    bind(ExecutorLoader.class).to(JdbcExecutorLoader.class);
    bind(MetricRegistry.class).in(Scopes.SINGLETON);
  }

  public Class<? extends Storage> resolveStorageClassType() {
    final StorageImplementationType type = StorageImplementationType
        .from(this.config.getStorageImplementation());
    if (type != null) {
      return type.getImplementationClass();
    } else {
      return loadCustomStorageClass(this.config.getStorageImplementation());
    }
  }

  private Class<? extends Storage> loadCustomStorageClass(final String storageImplementation) {
    try {
      return (Class<? extends Storage>) Class.forName(storageImplementation);
    } catch (final ClassNotFoundException e) {
      throw new StorageException(e);
    }
  }

  // todo kunkun-tang: the below method should moved out to azkaban-db module eventually.
  @Inject
  @Provides
  @Singleton
  public AzkabanDataSource getDataSource(final Props props) {
    final String databaseType = props.getString("database.type");

    if (databaseType.equals("h2")) {
      final String path = props.getString("h2.path");
      final Path h2DbPath = Paths.get(path).toAbsolutePath();
      log.info("h2 DB path: " + h2DbPath);
      return new H2FileDataSource(h2DbPath);
    }
    final int port = props.getInt("mysql.port");
    final String host = props.getString("mysql.host");
    final String database = props.getString("mysql.database");
    final String user = props.getString("mysql.user");
    final String password = props.getString("mysql.password");
    final int numConnections = props.getInt("mysql.numconnections");

    return new MySQLDataSource(host, port, database, user, password, numConnections);
  }

  @Inject
  @Provides
  @Singleton
  public Configuration createHadoopConfiguration() {
    final String hadoopConfDirPath = requireNonNull(this.props.get(HADOOP_CONF_DIR_PATH));

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
  public FileSystem createHadoopFileSystem(final Configuration hadoopConf, final HdfsAuth auth) {
    try {
      auth.authorize();
      return FileSystem.get(hadoopConf);
    } catch (final IOException e) {
      log.error("Unable to initialize HDFS", e);
      throw new AzkabanException(e);
    }
  }

  @Provides
  public QueryRunner createQueryRunner(final AzkabanDataSource dataSource) {
    return new QueryRunner(dataSource);
  }
}
