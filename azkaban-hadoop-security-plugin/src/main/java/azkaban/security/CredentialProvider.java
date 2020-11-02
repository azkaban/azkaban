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

package azkaban.security;

import java.security.KeyStore;

/**
 * Fetch and inject user secret key into custom credential object.
 */
public interface CredentialProvider {

  /**
   * register custom secret keys on behalf of an user into credentials.
   *
   * @param user find out the user's credential and register it.
   */
  public void register(String user);

  /**
   * get KeyStore to be reused within an Azkaban Flow
   *
   * @return KeyStore
   */
  public KeyStore getKeyStore();

  /**
   * set KeyStore
   *
   * @param keyStore Object
   */
  public void setKeyStore(final KeyStore keyStore);
}
