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

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.db.DatabaseOperator;
import azkaban.project.JdbcProjectImpl;
import azkaban.spi.Storage;
import azkaban.storage.DatabaseStorage;
import azkaban.storage.HdfsStorage;
import azkaban.storage.LocalStorage;
import azkaban.storage.ProjectStorageManager;
import azkaban.utils.Props;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;


public class ServiceProviderTest {

  private static final String AZKABAN_LOCAL_TEST_STORAGE = "AZKABAN_LOCAL_TEST_STORAGE";
  private static final String AZKABAN_TEST_HDFS_STORAGE_TYPE = "HDFS";
  private static final String AZKABAN_TEST_STORAGE_HDFS_URI= "hdfs://test.com:9000/azkaban/";

  // Test if one class is singletonly guiced. could be called by
  // AZ Common, Web, or Exec Modules.
  public static void assertSingleton(final Class azkabanClass, final Injector injector) {
    final Object azkabanObj1 = injector.getInstance(azkabanClass);
    final Object azkabanObj2 = injector.getInstance(azkabanClass);
    // Note: isSameAs is quite different from isEqualto in AssertJ
    assertThat(azkabanObj1).isSameAs(azkabanObj2).isNotNull();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(AZKABAN_LOCAL_TEST_STORAGE));
  }

  @Test
  public void testInjections() throws Exception {
    final Props props = new Props();
    props.put("database.type", "h2");
    props.put("h2.path", "h2");
    props
        .put(Constants.ConfigurationKeys.AZKABAN_STORAGE_LOCAL_BASEDIR, AZKABAN_LOCAL_TEST_STORAGE);

    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props)
    );
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(injector);

    assertThat(injector.getInstance(JdbcProjectImpl.class)).isNotNull();
    assertThat(injector.getInstance(ProjectStorageManager.class)).isNotNull();
    assertThat(injector.getInstance(DatabaseStorage.class)).isNotNull();
    assertThat(injector.getInstance(LocalStorage.class)).isNotNull();
    assertThatThrownBy(
        () -> injector.getInstance(HdfsStorage.class))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Guice configuration errors");

    assertThat(injector.getInstance(Storage.class)).isNotNull();
    assertThat(injector.getInstance(DatabaseOperator.class)).isNotNull();
  }

  @Test
  public void testHadoopInjection() throws Exception {
    final Props props = new Props();
    props.put("database.type", "h2");
    props.put("h2.path", "h2");
    props.put(Constants.ConfigurationKeys.AZKABAN_STORAGE_TYPE, AZKABAN_TEST_HDFS_STORAGE_TYPE);
    props.put(Constants.ConfigurationKeys.HADOOP_CONF_DIR_PATH, "./");
    props.put(Constants.ConfigurationKeys.AZKABAN_STORAGE_HDFS_ROOT_URI, AZKABAN_TEST_STORAGE_HDFS_URI);

    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props)
    );
    SERVICE_PROVIDER.unsetInjector();
    SERVICE_PROVIDER.setInjector(injector);

    assertSingleton(HdfsStorage.class, injector);
    assertThat(injector.getInstance(Storage.class)).isNotNull();
  }
}
