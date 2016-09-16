/*
 * Copyright 2011 LinkedIn Corp.
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

package azkaban.security.commons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;

import azkaban.utils.Props;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

public class SecurityUtils {
  // Secure Hadoop proxy user params
  public static final String ENABLE_PROXYING = "azkaban.should.proxy"; // boolean
  public static final String PROXY_KEYTAB_LOCATION = "proxy.keytab.location";
  public static final String PROXY_USER = "proxy.user";
  public static final String TO_PROXY = "user.to.proxy";

  private static UserGroupInformation loginUser = null;

  /**
   * Create a proxied user based on the explicit user name, taking other
   * parameters necessary from properties file.
   */
  public static synchronized UserGroupInformation getProxiedUser(
      String toProxy, Properties prop, Logger log, Configuration conf)
      throws IOException {

    if (conf == null) {
      throw new IllegalArgumentException("conf can't be null");
    }
    UserGroupInformation.setConfiguration(conf);

    if (toProxy == null) {
      throw new IllegalArgumentException("toProxy can't be null");
    }

    if (loginUser == null) {
      log.info("No login user. Creating login user");
      String keytab = verifySecureProperty(prop, PROXY_KEYTAB_LOCATION, log);
      String proxyUser = verifySecureProperty(prop, PROXY_USER, log);
      UserGroupInformation.loginUserFromKeytab(proxyUser, keytab);
      loginUser = UserGroupInformation.getLoginUser();
      log.info("Logged in with user " + loginUser);
    } else {
      log.info("loginUser (" + loginUser + ") already created, refreshing tgt.");
      loginUser.checkTGTAndReloginFromKeytab();
    }

    return UserGroupInformation.createProxyUser(toProxy, loginUser);
  }

  /**
   * Create a proxied user, taking all parameters, including which user to proxy
   * from provided Properties.
   */
  public static UserGroupInformation getProxiedUser(Properties prop,
      Logger log, Configuration conf) throws IOException {
    String toProxy = verifySecureProperty(prop, TO_PROXY, log);
    UserGroupInformation user = getProxiedUser(toProxy, prop, log, conf);
    if (user == null)
      throw new IOException(
          "Proxy as any user in unsecured grid is not supported!"
              + prop.toString());
    log.info("created proxy user for " + user.getUserName() + user.toString());
    return user;
  }

  public static String verifySecureProperty(Properties properties, String s,
      Logger l) throws IOException {
    String value = properties.getProperty(s);

    if (value == null)
      throw new IOException(s
          + " not set in properties. Cannot use secure proxy");
    l.info("Secure proxy configuration: Property " + s + " = " + value);
    return value;
  }

  public static boolean shouldProxy(Properties prop) {
    String shouldProxy = prop.getProperty(ENABLE_PROXYING);

    return shouldProxy != null && shouldProxy.equals("true");
  }

  public static final String OBTAIN_BINARY_TOKEN = "obtain.binary.token";
  public static final String MAPREDUCE_JOB_CREDENTIALS_BINARY =
      "mapreduce.job.credentials.binary";

  public static synchronized void prefetchToken(final File tokenFile,
      final Props p, final Logger logger) throws InterruptedException,
      IOException {

    final Configuration conf = new Configuration();
    logger.info("Getting proxy user for " + p.getString(TO_PROXY));
    logger.info("Getting proxy user for " + p.toString());

    getProxiedUser(p.toProperties(), logger, conf).doAs(
        new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            getToken(p);
            return null;
          }

          private void getToken(Props p) throws InterruptedException,
              IOException {
            String shouldPrefetch = p.getString(OBTAIN_BINARY_TOKEN);
            if (shouldPrefetch != null && shouldPrefetch.equals("true")) {
              logger.info("Pre-fetching token");

              logger.info("Pre-fetching fs token");
              FileSystem fs = FileSystem.get(conf);
              Token<?> fsToken =
                  fs.getDelegationToken(p.getString("user.to.proxy"));
              logger.info("Created token: " + fsToken.toString());

              Job job =
                  new Job(conf, "totally phony, extremely fake, not real job");
              JobConf jc = new JobConf(conf);
              JobClient jobClient = new JobClient(jc);
              logger.info("Pre-fetching job token: Got new JobClient: " + jc);
              Token<DelegationTokenIdentifier> mrdt =
                  jobClient.getDelegationToken(new Text("hi"));
              logger.info("Created token: " + mrdt.toString());

              job.getCredentials().addToken(new Text("howdy"), mrdt);
              job.getCredentials().addToken(fsToken.getService(), fsToken);

              FileOutputStream fos = null;
              DataOutputStream dos = null;
              try {
                fos = new FileOutputStream(tokenFile);
                dos = new DataOutputStream(fos);
                job.getCredentials().writeTokenStorageToStream(dos);
              } finally {
                if (dos != null) {
                  dos.close();
                }
                if (fos != null) {
                  fos.close();
                }
              }
              logger.info("Loading hadoop tokens into "
                  + tokenFile.getAbsolutePath());
              p.put("HadoopTokenFileLoc", tokenFile.getAbsolutePath());
            } else {
              logger.info("Not pre-fetching token");
            }
          }
        });
  }
}
