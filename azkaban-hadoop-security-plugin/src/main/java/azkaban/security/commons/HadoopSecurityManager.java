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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import azkaban.utils.Props;

public abstract class HadoopSecurityManager {

  public static final String ENABLE_PROXYING = "azkaban.should.proxy"; // boolean

  public static final String PROXY_KEYTAB_LOCATION = "proxy.keytab.location";
  public static final String PROXY_USER = "proxy.user";
  public static final String USER_TO_PROXY = "user.to.proxy";
  public static final String OBTAIN_BINARY_TOKEN = "obtain.binary.token";
  public static final String MAPREDUCE_JOB_CREDENTIALS_BINARY =
      "mapreduce.job.credentials.binary";

  public static final String OBTAIN_JOBTRACKER_TOKEN =
      "obtain.jobtracker.token";
  public static final String OBTAIN_NAMENODE_TOKEN = "obtain.namenode.token";
  public static final String OBTAIN_HCAT_TOKEN = "obtain.hcat.token";

  public boolean isHadoopSecurityEnabled()
      throws HadoopSecurityManagerException {
    return false;
  }

  public void reloginFromKeytab() throws IOException {
    UserGroupInformation.getLoginUser().reloginFromKeytab();
  }

  /**
   * Create a proxied user based on the explicit user name, taking other
   * parameters necessary from properties file.
   */
  public abstract UserGroupInformation getProxiedUser(String toProxy)
      throws HadoopSecurityManagerException;

  /**
   * Create a proxied user, taking all parameters, including which user to proxy
   * from provided Properties.
   */
  public abstract UserGroupInformation getProxiedUser(Props prop)
      throws HadoopSecurityManagerException;

  public abstract FileSystem getFSAsUser(String user)
      throws HadoopSecurityManagerException;

  public static boolean shouldProxy(Properties prop) {
    String shouldProxy = prop.getProperty(ENABLE_PROXYING);

    return shouldProxy != null && shouldProxy.equals("true");
  }

  public abstract void prefetchToken(File tokenFile, String userToProxy,
      Logger logger) throws HadoopSecurityManagerException;

  public abstract void cancelTokens(File tokenFile, String userToProxy,
      Logger logger) throws HadoopSecurityManagerException;

  public abstract void prefetchToken(File tokenFile, Props props, Logger logger)
      throws HadoopSecurityManagerException;

}
