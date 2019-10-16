/*
 * Copyright 2019 LinkedIn Corp.
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

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import java.io.File;
import org.apache.log4j.Logger;
import azkaban.flow.CommonJobProperties;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.Props;


/**
 * Hadoop Proxy
 */
public class HadoopProxy {

  private HadoopSecurityManager hadoopSecurityManager;
  private boolean shouldProxy = false;
  private boolean obtainTokens = false;
  private String userToProxy = null;
  private File tokenFile = null;

  public HadoopProxy(Props sysProps, Props jobProps, final Logger logger) {
    init(sysProps, jobProps, logger);
  }

  public boolean isProxyEnabled() {
    return this.shouldProxy && this.obtainTokens;
  }

  /**
   * Initialize the Hadoop Proxy Object
   *
   * @param sysProps system properties
   * @param jobProps job properties
   * @param logger logger handler
   */
  private void init(Props sysProps, Props jobProps, final Logger logger) {
    shouldProxy = sysProps.getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    jobProps.put(HadoopSecurityManager.ENABLE_PROXYING, Boolean.toString(shouldProxy));
    obtainTokens = sysProps.getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, false);
    if (shouldProxy) {
      logger.info("Initiating hadoop security manager.");
      try {
        hadoopSecurityManager = HadoopJobUtils.loadHadoopSecurityManager(sysProps, logger);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to get hadoop security manager!"
            + e.getCause());
      }
    }
  }

  /**
   * Setup Job Properties when the proxy is enabled
   *
   * @param props all properties
   * @param jobProps job properties
   * @param logger logger handler
   */
  public void setupPropsForProxy(Props props, Props jobProps, final Logger logger)
      throws Exception {
    if (isProxyEnabled()) {
      userToProxy = jobProps.getString(HadoopSecurityManager.USER_TO_PROXY);
      logger.info("Need to proxy. Getting tokens.");
      // get tokens in to a file, and put the location in props
      tokenFile = HadoopJobUtils.getHadoopTokens(hadoopSecurityManager, props, logger);
      jobProps.put("env." + HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
    }
  }

  /**
   * Generate JVM Proxy Secure Argument
   *
   * @param sysProps system properties
   * @param jobProps job properties
   * @param logger logger handler
   * @return proxy secure JVM argument string
   */
  public String getJVMArgument(Props sysProps, Props jobProps, final Logger logger) {
    String secure = "";

    if (shouldProxy) {
      logger.info("Setting up secure proxy info for child process");
      secure = " -D" + HadoopSecurityManager.USER_TO_PROXY
          + "=" + jobProps.getString(HadoopSecurityManager.USER_TO_PROXY);

      String extraToken = sysProps.getString(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, "false");
      if (extraToken != null) {
        secure += " -D" + HadoopSecurityManager.OBTAIN_BINARY_TOKEN + "=" + extraToken;
      }
      logger.info("Secure settings = " + secure);
    } else {
      logger.info("Not setting up secure proxy info for child process");
    }

    return secure;
  }

  /**
   * Cancel Hadoop Tokens
   *
   * @param logger logger handler
   */
  public void cancelHadoopTokens(final Logger logger) {
    if (tokenFile == null) {
      return;
    }
    try {
      hadoopSecurityManager.cancelTokens(tokenFile, userToProxy, logger);
    } catch (HadoopSecurityManagerException e) {
      logger.error(e.getCause() + e.getMessage());
    } catch (Exception e) {
      logger.error(e.getCause() + e.getMessage());
    }
    if (tokenFile.exists()) {
      tokenFile.delete();
    }
  }

  /**
   * Kill all Spawned Hadoop Jobs
   *
   * @param jobProps job properties
   * @param logger logger handler
   */
  public void killAllSpawnedHadoopJobs(Props jobProps, final Logger logger) {
    if (obtainTokens && tokenFile == null) {
      return; // do null check for tokenFile
    }

    final String logFilePath = jobProps.getString(CommonJobProperties.JOB_LOG_FILE);
    logger.info("Log file path is: " + logFilePath);

    HadoopJobUtils.proxyUserKillAllSpawnedHadoopJobs(logFilePath, jobProps, tokenFile, logger);
  }
}
