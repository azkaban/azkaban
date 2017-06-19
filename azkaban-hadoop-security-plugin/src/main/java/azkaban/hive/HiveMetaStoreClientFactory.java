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

import com.google.common.base.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
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
 * An implementation of {@link BasePooledObjectFactory} for {@link IMetaStoreClient}.
 */
public class HiveMetaStoreClientFactory extends BasePooledObjectFactory<IMetaStoreClient> {

  public static final String HIVE_METASTORE_TOKEN_SIGNATURE = "hive.metastore.token.signature";
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientFactory.class);
  private final HiveConf hiveConf;

  public HiveMetaStoreClientFactory(final Optional<String> hcatURI) {
    this(getHiveConf(hcatURI));
  }

  public HiveMetaStoreClientFactory(final HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public HiveMetaStoreClientFactory() {
    this(Optional.<String>absent());
  }

  private static HiveConf getHiveConf(final Optional<String> hcatURI) {
    final HiveConf hiveConf = new HiveConf();
    if (hcatURI.isPresent() && StringUtils.isNotBlank(hcatURI.get())) {
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hcatURI.get());
      hiveConf.set(HIVE_METASTORE_TOKEN_SIGNATURE, hcatURI.get());
    }
    return hiveConf;
  }

  private IMetaStoreClient createMetaStoreClient() throws MetaException {
    final HiveMetaHookLoader hookLoader = tbl -> {
      if (tbl == null) {
        return null;
      }

      try {
        final HiveStorageHandler storageHandler =
            HiveUtils.getStorageHandler(HiveMetaStoreClientFactory.this.hiveConf,
                tbl.getParameters().get(META_TABLE_STORAGE));
        return storageHandler == null ? null : storageHandler.getMetaHook();
      } catch (final HiveException e) {
        LOG.error(e.toString());
        throw new MetaException("Failed to get storage handler: " + e);
      }
    };

    return RetryingMetaStoreClient
        .getProxy(this.hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
  }

  @Override
  public IMetaStoreClient create() {
    try {
      return createMetaStoreClient();
    } catch (final MetaException e) {
      throw new RuntimeException("Unable to create " + IMetaStoreClient.class.getSimpleName(), e);
    }
  }

  @Override
  public PooledObject<IMetaStoreClient> wrap(final IMetaStoreClient client) {
    return new DefaultPooledObject<>(client);
  }

  @Override
  public void destroyObject(final PooledObject<IMetaStoreClient> client) {
    client.getObject().close();
  }

}
