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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.security.KeyStore;

public class KeyStoreManagerTest {

  private KeyStoreManager keyStoreManager;
  private KeyStore keyStore;

  @Before
  public void setup() {
    this.keyStore = null;
    keyStoreManager = KeyStoreManager.getInstance();
  }

  /**
   * Since the KeyStore is not set, it must be null
   */
  @Test
  public void getKeyStoreNull() {
    final KeyStore keyStore = keyStoreManager.getKeyStore();
    Assert.assertNull(keyStore);
  }

  /**
   * KeyStore is set, it must not be null, and must be same as input when retrieved.
   */
  @Test
  public void getKeyStoreNotNull() {
    this.keyStore = Mockito.mock(KeyStore.class);
    keyStoreManager.setKeyStore(this.keyStore);
    final KeyStore keyStore1 = keyStoreManager.getKeyStore();
    Assert.assertNotNull(keyStore1);
    Assert.assertEquals(this.keyStore, keyStore1);
  }
}
