/*
 * Copyright 2020 LinkedIn Corp.
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
 */

package azkaban.executor;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.security.KeyStore;

/**
 * KeyStoreManger is responsible to manage the KeyStore used during containerized execution of a
 * flow. The HadoopSecurityManager must provide the KeyStore to CredentialProvider
 */
public class KeyStoreManager {
  private static volatile KeyStoreManager ksmInstance = null;
  private static final Logger logger = LoggerFactory.getLogger(KeyStoreManager.class);

  private KeyStore keyStore = null;

  public static KeyStoreManager getInstance() {
    if (ksmInstance == null) {
      synchronized (KeyStoreManager.class) {
        if (ksmInstance == null) {
          logger.info("Getting a new instance of KeyStoreManager");
          ksmInstance = new KeyStoreManager();
        }
      }
    }
    else {
      logger.info("Getting existing instance of KeyStoreManager");
    }
    return ksmInstance;
  }

  /**
   * Gets the cached KeyStore object
   * @return Cached KeyStore object.
   */
  public KeyStore getKeyStore() {
    return this.keyStore;
  }

  /**
   *
   * @param keyStore Must be non-null.
   */
  public void setKeyStore(final @Nonnull KeyStore keyStore) {
    this.keyStore = keyStore;
  }
}
