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

import azkaban.db.DatabaseOperator;
import azkaban.db.DatabaseOperatorImpl;

import azkaban.executor.ExecutorLoader;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.project.JdbcProjectLoader;
import azkaban.project.ProjectLoader;
import azkaban.spi.Storage;
import azkaban.spi.StorageException;
import azkaban.storage.LocalStorage;
import azkaban.storage.StorageImplementationType;
import azkaban.utils.Props;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import java.io.File;
import org.apache.commons.dbutils.QueryRunner;


/**
 * This Guice module is currently a one place container for all bindings in the current module. This is intended to
 * help during the migration process to Guice. Once this class starts growing we can move towards more modular
 * structuring of Guice components.
 */
public class AzkabanCommonModule extends AbstractModule {
  private final AzkabanCommonModuleConfig config;

  public AzkabanCommonModule(Props props) {
    this.config = new AzkabanCommonModuleConfig(props);
  }

  @Override
  protected void configure() {
    bind(ExecutorLoader.class).to(JdbcExecutorLoader.class).in(Scopes.SINGLETON);
    bind(ProjectLoader.class).to(JdbcProjectLoader.class).in(Scopes.SINGLETON);
    bind(Props.class).toInstance(config.getProps());
    bind(Storage.class).to(resolveStorageClassType()).in(Scopes.SINGLETON);
    bind(DatabaseOperator.class).to(DatabaseOperatorImpl.class).in(Scopes.SINGLETON);
    //todo kunkun-tang : Consider both H2 DataSource and MysqlDatasource case.
    bind(QueryRunner.class).toInstance(config.getQueryRunner());
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

  @Inject
  public @Provides
  LocalStorage createLocalStorage(AzkabanCommonModuleConfig config) {
    return new LocalStorage(new File(config.getLocalStorageBaseDirPath()));
  }
}
