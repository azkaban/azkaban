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
import com.google.inject.Singleton;
import java.io.File;

import static azkaban.storage.StorageImplementationType.*;


public class AzkabanCommonModule extends AbstractModule {
  public static final String AZKABAN_STORAGE_TYPE = "azkaban.storage.type";

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

  public AzkabanCommonModule(Props props) {
    this.props = props;
    this.storageImplementation = props.getString(AZKABAN_STORAGE_TYPE, LOCAL.name());
  }

  @Override
  protected void configure() {
    bind(ProjectLoader.class).to(JdbcProjectLoader.class).in(Scopes.SINGLETON);
    bind(Props.class).toInstance(props);
    bind(Storage.class).to(resolveStorageClassType()).in(Scopes.SINGLETON);
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
  @Singleton
  LocalStorage createLocalStorage(StorageConfig config) {
    return new LocalStorage(new File(config.getBaseDirectoryPath()));
  }
}
