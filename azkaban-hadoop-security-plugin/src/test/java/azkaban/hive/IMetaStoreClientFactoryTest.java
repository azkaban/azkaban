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
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.Test;

public class IMetaStoreClientFactoryTest {

  static final File METASTORE_DB_DIR = new File("metastore_db");
  static final File DERBY_LOG_FILE = new File("derby.log");

  @Test
  public void testCreate() throws TException, IOException {
    cleanup();

    final IMetaStoreClient msc = new IMetaStoreClientFactory(new HiveConf()).create();

    assertThat(msc instanceof RetryingMetaStoreClient);

    final String DB_NAME = "test_db";
    final String DB_DESCRIPTION = "test database";
    final String DB_LOCATION = "file:/tmp/" + DB_NAME;

    Database db = new Database(DB_NAME, DB_DESCRIPTION, DB_LOCATION, null);

    msc.dropDatabase(DB_NAME, true, true);
    msc.createDatabase(db);
    db = msc.getDatabase(DB_NAME);

    assertThat(db.getName()).isEqualTo(DB_NAME);
    assertThat(db.getDescription()).isEqualTo(DB_DESCRIPTION);
    assertThat(db.getLocationUri()).isEqualTo(DB_LOCATION);

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
