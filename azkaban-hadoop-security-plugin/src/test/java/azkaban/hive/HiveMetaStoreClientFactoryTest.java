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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

public class HiveMetaStoreClientFactoryTest {

  @Test
  public void testCreate() throws TException {
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
    Assert.assertEquals(db.getName(), dbName);
    Assert.assertEquals(db.getDescription(), description);
    Assert.assertEquals(db.getLocationUri(), location);
  }
}
