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
import azkaban.utils.Props;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;


/**
 * This class helps in HDFS authorization and is a wrapper over Hadoop's {@link
 * UserGroupInformation} class.
 */
@Singleton
public class HdfsAuth {
  private static final Logger log = Logger.getLogger(HdfsAuth.class);

  private final boolean isSecurityEnabled;

  private UserGroupInformation loggedInUser = null;
  private String keytabPath = null;
  private String keytabPrincipal = null;

  @Inject
  public HdfsAuth(final Props props, @Named("hdfsConf") final Configuration conf) {
    UserGroupInformation.setConfiguration(conf);
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    if (this.isSecurityEnabled) {
      log.info("The Hadoop cluster has enabled security");
      this.keytabPath = requireNonNull(props.getString(AZKABAN_KEYTAB_PATH));
      this.keytabPrincipal = requireNonNull(props.getString(AZKABAN_KERBEROS_PRINCIPAL));
    }
  }

  /**
   * API to authorize HDFS access. This logins in the configured user via the keytab. If the user is
   * already logged in then it renews the TGT.
   */
  public void authorize() {
    if (this.isSecurityEnabled) {
      try {
        login(this.keytabPrincipal, this.keytabPath);
      } catch (final IOException e) {
        log.error(e);
        throw new AzkabanException(String.format(
            "Error: Unable to authorize to Hadoop. Principal: %s Keytab: %s", this.keytabPrincipal,
            this.keytabPath));
      }
    }
  }

  private void login(final String keytabPrincipal, final String keytabPath) throws IOException {
    if (this.loggedInUser == null) {
      log.info(
          String.format("Logging in using Principal: %s Keytab: %s", keytabPrincipal, keytabPath));

      UserGroupInformation.loginUserFromKeytab(keytabPrincipal, keytabPath);
      this.loggedInUser = UserGroupInformation.getLoginUser();
      log.info(String.format("User %s logged in.", this.loggedInUser));
    } else {
      log.info(String.format("User %s already logged in. Refreshing TGT", this.loggedInUser));
      this.loggedInUser.checkTGTAndReloginFromKeytab();
    }
  }
}
