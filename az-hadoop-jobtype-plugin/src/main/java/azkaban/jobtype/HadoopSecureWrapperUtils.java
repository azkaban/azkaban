/*
 * Copyright 2015 LinkedIn Corp.
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
package azkaban.jobtype;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import azkaban.jobExecutor.ProcessJob;
import azkaban.security.commons.HadoopSecurityManager;


/**
 * <pre>
 * There are many common methods that's required by the HadoopSecure*Wrapper.java's. They are all consolidated
 * here.
 * </pre>
 *
 * @see HadoopSecurePigWrapper
 * @see HadoopSecureHiveWrapper
 * @see HadoopSecureSparkWrapper
 */
public class HadoopSecureWrapperUtils {

  /**
   * Perform all the magic required to get the proxyUser in a securitized grid
   *
   * @return a UserGroupInformation object for the specified userToProxy, which will also contain
   * the logged in user's tokens
   */
  private static UserGroupInformation createSecurityEnabledProxyUser(String userToProxy,
      String fileLocation, Logger log
  ) throws IOException {

    if (!new File(fileLocation).exists()) {
      throw new RuntimeException("hadoop token file doesn't exist.");
    }

    log.info("Found token file.  Setting " + HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY
        + " to " + fileLocation);
    System.setProperty(HadoopSecurityManager.MAPREDUCE_JOB_CREDENTIALS_BINARY, fileLocation);

    UserGroupInformation loginUser = null;

    loginUser = UserGroupInformation.getLoginUser();
    log.info("Current logged in user is " + loginUser.getUserName());

    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(userToProxy, loginUser);

    for (Token<?> token : loginUser.getTokens()) {
      proxyUser.addToken(token);
    }
    proxyUser.addCredentials(loginUser.getCredentials());
    return proxyUser;
  }

  /**
   * Sets up the UserGroupInformation proxyUser object so that calling code can do doAs returns null
   * if the jobProps does not call for a proxyUser
   *
   * @param jobProps job properties
   * @param tokenFile pass tokenFile if known. Pass null if the tokenFile is in the environmental
   * variable
   * already.
   * @return returns null if no need to run as proxyUser, otherwise returns valid proxyUser that can
   * doAs
   */
  public static UserGroupInformation setupProxyUser(Properties jobProps,
      String tokenFile, Logger log) {
    UserGroupInformation proxyUser = null;

    if (!HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
      log.info("submitting job as original submitter, not proxying");
      return proxyUser;
    }

    // set up hadoop related configurations
    final Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
    boolean securityEnabled = UserGroupInformation.isSecurityEnabled();

    // setting up proxy user if required
    try {
      String userToProxy = null;
      userToProxy = jobProps.getProperty(HadoopSecurityManager.USER_TO_PROXY);
      if (securityEnabled) {
        proxyUser =
            HadoopSecureWrapperUtils.createSecurityEnabledProxyUser(
                userToProxy, tokenFile, log);
        log.info("security enabled, proxying as user " + userToProxy);
      } else {
        proxyUser = UserGroupInformation.createRemoteUser(userToProxy);
        log.info("security not enabled, proxying as user " + userToProxy);
      }
    } catch (IOException e) {
      log.error("HadoopSecureWrapperUtils.setupProxyUser threw an IOException",
          e);
    }

    return proxyUser;
  }

  /**
   * Loading the properties file, which is a combination of the jobProps file and sysProps file
   *
   * @return a Property file, which is the combination of the jobProps file and sysProps file
   */

  @SuppressWarnings("DefaultCharset")
  public static Properties loadAzkabanProps() throws IOException {
    String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
    Properties props = new Properties();
    props.load(new BufferedReader(new FileReader(propsFile)));
    return props;
  }

  /**
   * Looks for particular properties inside the Properties object passed in, and determines whether
   * proxying should happen or not
   *
   * @return a boolean value of whether the job should proxy or not
   */
  public static boolean shouldProxy(Properties props) {
    String shouldProxy = props.getProperty(HadoopSecurityManager.ENABLE_PROXYING);
    return shouldProxy != null && shouldProxy.equals("true");
  }
}
