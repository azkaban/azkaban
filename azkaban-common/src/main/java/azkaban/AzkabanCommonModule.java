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

import azkaban.Constants.ConfigurationKeys;
import azkaban.db.AzkabanDataSource;
import azkaban.db.H2FileDataSource;
import azkaban.db.MySQLDataSource;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.ProjectCache;
import azkaban.project.ProjectCacheInMem;
import azkaban.project.ProjectLoader;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.storage.StorageImplementationType;
import azkaban.trigger.JdbcTriggerImpl;
import azkaban.trigger.TriggerLoader;
import azkaban.utils.OsCpuUtil;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.commons.dbutils.QueryRunner;
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
    install(new AzkabanCoreModule(this.props));
    bind(Storage.class).to(resolveStorageClassType());
    bind(AzkabanDataSource.class).to(resolveDataSourceType());
    bind(TriggerLoader.class).to(JdbcTriggerImpl.class);
    bind(ProjectLoader.class).to(JdbcProjectImpl.class);
    bind(ExecutorLoader.class).to(JdbcExecutorLoader.class);
    bind(ProjectCache.class).to(ProjectCacheInMem.class);
    bind(OsCpuUtil.class).toProvider(() -> {
      final int cpuLoadPeriodSec = this.props
          .getInt(ConfigurationKeys.AZKABAN_POLLING_CRITERIA_CPU_LOAD_PERIOD_SEC,
              Constants.DEFAULT_AZKABAN_POLLING_CRITERIA_CPU_LOAD_PERIOD_SEC);
      final int pollingIntervalMs = this.props
          .getInt(ConfigurationKeys.AZKABAN_POLLING_INTERVAL_MS,
              Constants.DEFAULT_AZKABAN_POLLING_INTERVAL_MS);
      return new OsCpuUtil(Math.max(1, (cpuLoadPeriodSec * 1000) / pollingIntervalMs));
    });
  }

  public Class<? extends Storage> resolveStorageClassType() {
    final StorageImplementationType type = StorageImplementationType
        .from(this.config.getStorageImplementation());
    if (type == StorageImplementationType.HDFS || type == StorageImplementationType.LOCAL_HADOOP) {
      install(new HadoopModule(this.props));
    }
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

  private Class<? extends AzkabanDataSource> resolveDataSourceType() {

    final String databaseType = this.props.getString("database.type");
    if (databaseType.equals("h2")) {
      return H2FileDataSource.class;
    } else {
      return MySQLDataSource.class;
    }
  }

  @Provides
  public QueryRunner createQueryRunner(final AzkabanDataSource dataSource) {
    return new QueryRunner(dataSource);
  }
}
