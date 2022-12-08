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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_ENABLED;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_OFFLINE_LOGS_LOADER_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_OFFLINE_LOGS_LOADER_ENABLED;
import static azkaban.Constants.ImageMgmtConstants.IMAGE_RAMPUP_PLAN;
import static azkaban.Constants.ImageMgmtConstants.IMAGE_TYPE;
import static azkaban.Constants.ImageMgmtConstants.IMAGE_VERSION;
import static azkaban.Constants.LogConstants.NEARLINE_LOGS;
import static azkaban.Constants.LogConstants.OFFLINE_LOGS;

import azkaban.Constants.ConfigurationKeys;
import azkaban.db.AzkabanDataSource;
import azkaban.db.H2FileDataSource;
import azkaban.db.MySQLDataSource;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.imagemgmt.converters.Converter;
import azkaban.imagemgmt.converters.ImageRampupPlanConverter;
import azkaban.imagemgmt.converters.ImageTypeConverter;
import azkaban.imagemgmt.converters.ImageVersionConverter;
import azkaban.imagemgmt.daos.ImageMgmtCommonDao;
import azkaban.imagemgmt.daos.ImageMgmtCommonDaoImpl;
import azkaban.imagemgmt.daos.ImageRampupDao;
import azkaban.imagemgmt.daos.ImageRampupDaoImpl;
import azkaban.imagemgmt.daos.ImageTypeDao;
import azkaban.imagemgmt.daos.ImageTypeDaoImpl;
import azkaban.imagemgmt.daos.ImageVersionDao;
import azkaban.imagemgmt.daos.ImageVersionDaoImpl;
import azkaban.imagemgmt.daos.RampRuleDao;
import azkaban.imagemgmt.daos.RampRuleDaoImpl;
import azkaban.imagemgmt.rampup.ImageRampupManager;
import azkaban.imagemgmt.rampup.ImageRampupManagerImpl;
import azkaban.logs.JdbcExecutionLogsLoader;
import azkaban.logs.ExecutionLogsLoader;
import azkaban.project.InMemoryProjectCache;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.ProjectCache;
import azkaban.project.ProjectLoader;
import azkaban.scheduler.MissedSchedulesManager;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.storage.StorageImplementationType;
import azkaban.trigger.JdbcTriggerImpl;
import azkaban.trigger.TriggerLoader;
import azkaban.utils.OsCpuUtil;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This Guice module is currently a one place container for all bindings in the current module. This
 * is intended to help during the migration process to Guice. Once this class starts growing we can
 * move towards more modular structuring of Guice components.
 */
public class AzkabanCommonModule extends AbstractModule {

  private static final Logger logger = LoggerFactory.getLogger(AzkabanCommonModule.class);

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
    bind(ExecutionLogsLoader.class).annotatedWith(Names.named(NEARLINE_LOGS)).to(
        JdbcExecutionLogsLoader.class);
    bindOfflineLogsLoader();
    bind(ProjectCache.class).to(InMemoryProjectCache.class);
    bind(OsCpuUtil.class).toProvider(() -> {
      final int cpuLoadPeriodSec = this.props
          .getInt(ConfigurationKeys.AZKABAN_POLLING_CRITERIA_CPU_LOAD_PERIOD_SEC,
              Constants.DEFAULT_AZKABAN_POLLING_CRITERIA_CPU_LOAD_PERIOD_SEC);
      final int pollingIntervalMs = this.props
          .getInt(ConfigurationKeys.AZKABAN_POLLING_INTERVAL_MS,
              Constants.DEFAULT_AZKABAN_POLLING_INTERVAL_MS);
      return new OsCpuUtil(Math.max(1, (cpuLoadPeriodSec * 1000) / pollingIntervalMs));
    });
    bindImageManagementDependencies();
    bind(MissedSchedulesManager.class).in(Scopes.SINGLETON);
  }

  public Class<? extends Storage> resolveStorageClassType() {
    final StorageImplementationType type = StorageImplementationType
        .from(this.config.getStorageImplementation());
    if (type == StorageImplementationType.HDFS || type == StorageImplementationType.LOCAL_HADOOP) {
      install(new HadoopModule());
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

  @Inject
  @Provides
  @Singleton
  public AzkabanEventReporter createAzkabanEventReporter() {
    final boolean eventReporterEnabled =
        this.props.getBoolean(AZKABAN_EVENT_REPORTING_ENABLED, false);

    if (!eventReporterEnabled) {
      logger.info("Event reporter is not enabled");
      return null;
    }

    final Class<?> eventReporterClass =
        this.props.getClass(AZKABAN_EVENT_REPORTING_CLASS_PARAM, null);
    if (eventReporterClass != null && eventReporterClass.getConstructors().length > 0) {
      this.logger.info("Loading event reporter class " + eventReporterClass.getName());
      try {
        final Constructor<?> eventReporterClassConstructor =
            eventReporterClass.getConstructor(Props.class);
        return (AzkabanEventReporter) eventReporterClassConstructor.newInstance(this.props);
      } catch (final InvocationTargetException e) {
        this.logger.error(e.getTargetException().getMessage());
        if (e.getTargetException() instanceof IllegalArgumentException) {
          throw new IllegalArgumentException(e);
        } else {
          throw new RuntimeException(e);
        }
      } catch (final Exception e) {
        this.logger.error("Could not instantiate EventReporter " + eventReporterClass.getName());
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private void bindOfflineLogsLoader() {
    final boolean eventReporterEnabled =
        this.props.getBoolean(AZKABAN_OFFLINE_LOGS_LOADER_ENABLED, false);
    ExecutionLogsLoader executionLogsLoader = null;

    if (!eventReporterEnabled) {
      logger.info("Offline logs loader is not enabled");
    } else {
      final Class<?> offlineLogsLoader =
          this.props.getClass(AZKABAN_OFFLINE_LOGS_LOADER_CLASS_PARAM, null);
      if (offlineLogsLoader != null && offlineLogsLoader.getConstructors().length > 0) {
        this.logger.info("Loading offline logs loader class " + offlineLogsLoader.getName());
        try {
          final Constructor<?> offlineLogsLoaderConstructor =
              offlineLogsLoader.getConstructor(Props.class);
          executionLogsLoader =
              (ExecutionLogsLoader) offlineLogsLoaderConstructor.newInstance(this.props);
        } catch (final Exception e) {
          this.logger.error("Could not instantiate OfflineLogsLoader " + offlineLogsLoader.getName(), e);
        }
      }
    }

    if (executionLogsLoader == null) {
      bind(ExecutionLogsLoader.class).annotatedWith(Names.named(OFFLINE_LOGS)).toProvider(Providers.of(null));
    } else {
      bind(ExecutionLogsLoader.class).annotatedWith(Names.named(OFFLINE_LOGS)).toInstance(executionLogsLoader);
    }
  }

  private void bindImageManagementDependencies() {
    if (isContainerizedDispatchMethodEnabled()) {
      bind(ImageTypeDao.class).to(ImageTypeDaoImpl.class).in(Scopes.SINGLETON);
      bind(ImageVersionDao.class).to(ImageVersionDaoImpl.class).in(Scopes.SINGLETON);
      bind(ImageRampupDao.class).to(ImageRampupDaoImpl.class).in(Scopes.SINGLETON);
      bind(ImageRampupManager.class).to(ImageRampupManagerImpl.class).in(Scopes.SINGLETON);
      bind(RampRuleDao.class).to(RampRuleDaoImpl.class).in(Scopes.SINGLETON);
      bind(ImageMgmtCommonDao.class).to(ImageMgmtCommonDaoImpl.class).in(Scopes.SINGLETON);
      bind(Converter.class).annotatedWith(Names.named(IMAGE_TYPE))
          .to(ImageTypeConverter.class).in(Scopes.SINGLETON);
      bind(Converter.class).annotatedWith(Names.named(IMAGE_VERSION))
          .to(ImageVersionConverter.class).in(Scopes.SINGLETON);
      bind(Converter.class).annotatedWith(Names.named(IMAGE_RAMPUP_PLAN))
          .to(ImageRampupPlanConverter.class).in(Scopes.SINGLETON);
    }
  }

  private boolean isContainerizedDispatchMethodEnabled() {
    return DispatchMethod.isContainerizedMethodEnabled(this.props
        .getString(Constants.ConfigurationKeys.AZKABAN_EXECUTION_DISPATCH_METHOD,
            DispatchMethod.PUSH.name()));
  }
}
