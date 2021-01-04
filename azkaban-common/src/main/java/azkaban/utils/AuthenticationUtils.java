/*
* Copyright 2018 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.utils;

import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The util class for hadoop authentication.
 */
public class AuthenticationUtils {

  private static final Logger logger = LoggerFactory.getLogger(AuthenticationUtils.class);

  public static HttpURLConnection loginAuthenticatedURL(final URL url, final String keytabPrincipal,
      final String keytabPath) throws Exception {

    logger.info(
        "Logging in URL: " + url.toString() + " using Principal: " + keytabPrincipal + ", Keytab: "
            + keytabPath);

    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

    if (loginUser == null) {
      UserGroupInformation.loginUserFromKeytab(keytabPrincipal, keytabPath);
      loginUser = UserGroupInformation.getLoginUser();
      logger.info("Logged in with user " + loginUser);
    } else {
      logger.info("Login user (" + loginUser + ") already created, refreshing tgt.");
      loginUser.checkTGTAndReloginFromKeytab();
    }

    final HttpURLConnection connection = loginUser.doAs(
        (PrivilegedExceptionAction<HttpURLConnection>) () -> {
          final Token token = new Token();
          return new AuthenticatedURL().openConnection(url, token);
        });

    return connection;
  }
}
