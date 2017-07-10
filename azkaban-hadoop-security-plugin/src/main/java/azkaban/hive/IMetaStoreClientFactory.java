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


import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import azkaban.spi.AzkabanException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for creating an {@link IMetaStoreClient} using the {@link RetryingMetaStoreClient}
 */
public class IMetaStoreClientFactory {

  private static final Logger log = LoggerFactory.getLogger(IMetaStoreClientFactory.class);
  private final HiveConf hiveConf;

  public IMetaStoreClientFactory(final HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public IMetaStoreClient create() {
    final HiveMetaHookLoader hookLoader = tbl -> {
      if (tbl == null) {
        return null;
      }

      try {
        final HiveStorageHandler storageHandler =
            HiveUtils.getStorageHandler(this.hiveConf, tbl.getParameters().get(META_TABLE_STORAGE));
        return storageHandler == null ? null : storageHandler.getMetaHook();
      } catch (final HiveException e) {
        log.error(e.getMessage(), e);
        throw new MetaException("Failed to get storage handler: " + e);
      }
    };
    try {
      return RetryingMetaStoreClient
          .getProxy(this.hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
    } catch (final MetaException e) {
      throw new AzkabanException("Unable to create " + IMetaStoreClient.class.getSimpleName(), e);
    }
  }
}
