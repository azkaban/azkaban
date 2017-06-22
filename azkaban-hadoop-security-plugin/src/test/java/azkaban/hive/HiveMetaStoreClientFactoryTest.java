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

package azkaban.hive;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.Test;

public class HiveMetaStoreClientFactoryTest {

  public static final File METASTORE_DB_DIR = new File("metastore_db");
  public static final File DERBY_LOG_FILE = new File("derby.log");

  @Test
  public void testCreate() throws TException, IOException {
    cleanup();

    final HiveConf hiveConf = new HiveConf();
    final HiveMetaStoreClientFactory factory = new HiveMetaStoreClientFactory(hiveConf);
    final IMetaStoreClient msc = factory.create();

    final String dbName = "test_db";
    final String description = "test database";
    final String location = "file:/tmp/" + dbName;
    Database db = new Database(dbName, description, location, null);

    msc.dropDatabase(dbName, true, true);
    msc.createDatabase(db);
    db = msc.getDatabase(dbName);
    
    assertThat(db.getName()).isEqualTo(dbName);
    assertThat(db.getDescription()).isEqualTo(description);
    assertThat(db.getLocationUri()).isEqualTo(location);

    // Clean up if the test is successful
    cleanup();
  }

  private void cleanup() throws IOException {
    if (METASTORE_DB_DIR.exists()) {
      FileUtils.deleteDirectory(METASTORE_DB_DIR);
    }
    if (DERBY_LOG_FILE.exists()) {
      DERBY_LOG_FILE.delete();
    }
  }
}
