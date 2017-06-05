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

package azkaban.storage;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_KERBEROS_PRINCIPAL;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_KEYTAB_PATH;
import static java.util.Objects.requireNonNull;

import azkaban.spi.AzkabanException;
import azkaban.spi.StorageException;
import azkaban.utils.Props;
import com.google.inject.Inject;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;


/**
 * This class helps in HDFS authorization and is a wrapper over Hadoop's {@link
 * UserGroupInformation} class.
 */
public class HdfsAuth {

  private static final Logger log = Logger.getLogger(HdfsAuth.class);

  private final boolean isSecurityEnabled;

  private String keytabPath = null;
  private String keytabPrincipal = null;

  private volatile UserGroupInformation loggedInUser = null;

  @Inject
  public HdfsAuth(final Props props, final Configuration conf) {
    UserGroupInformation.setConfiguration(conf);
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    if (this.isSecurityEnabled) {
      log.info("The Hadoop cluster has enabled security");
      this.keytabPath = requireNonNull(props.getString(AZKABAN_KEYTAB_PATH));
      this.keytabPrincipal = requireNonNull(props.getString(AZKABAN_KERBEROS_PRINCIPAL));
    }
  }

  /**
   * Wrapper for Hadoop doAs
   *
   * All Hadoop/HDFS access calls must be wrapped in a doAs call. This utility automatically
   * logs in the user/refreshes the TGT and performs action.run()
   *
   * @param action executable block of code which is to be run in the doAs scope
   * @param <T> return class type
   * @return object returned by action.run()
   */
  public <T> T doAs(final PrivilegedExceptionAction<T> action) {
    authorize();
    try {
      return requireNonNull(this.loggedInUser).doAs(action);
    } catch (final Exception e) {
      throw new StorageException(
          String.format("Error while performing doAs user: %s", this.loggedInUser), e);
    }
  }

  /**
   * API to authorize HDFS access.
   * This logins in the configured user via the keytab.
   * If the user is already logged in then it renews the TGT.
   */
  private void authorize() {
    if (this.isSecurityEnabled) {
      try {
        login();
      } catch (final IOException e) {
        log.error(e);
        throw new AzkabanException(String.format(
            "Error: Unable to authorize to Hadoop. Principal: %s Keytab: %s", this.keytabPrincipal,
            this.keytabPath));
      }
    }
  }

  private synchronized void login() throws IOException {
    if (this.loggedInUser == null) {
      log.info(
          String.format("Logging in using Principal: %s Keytab: %s", this.keytabPrincipal,
              this.keytabPath));

      UserGroupInformation.loginUserFromKeytab(this.keytabPrincipal, this.keytabPath);
      this.loggedInUser = UserGroupInformation.getLoginUser();
      log.info(String.format("User %s logged in.", this.loggedInUser));
    } else {
      log.info(
          String
              .format("User %s already logged in. Refreshing TGT if required", this.loggedInUser));
      this.loggedInUser.checkTGTAndReloginFromKeytab();
    }
  }
}
