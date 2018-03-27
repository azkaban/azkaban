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

package azkaban.security;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER;
import static azkaban.Constants.JobProperties.EXTRA_HCAT_CLUSTERS;
import static azkaban.Constants.JobProperties.EXTRA_HCAT_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import azkaban.Constants;
import azkaban.Constants.JobProperties;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.ExecuteAsUser;
import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Master;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.mapreduce.v2.api.HSClientProtocol;
import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class HadoopSecurityManager_H_2_0 extends HadoopSecurityManager {

  // Use azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER instead
  @Deprecated
  public static final String NATIVE_LIB_FOLDER = "azkaban.native.lib";
  /**
   * TODO: This should be exposed as a configurable parameter
   *
   * The assumption is that an "azkaban" group exists which has access to data created by the
   * azkaban process. For example, this may include delegation tokens created for other users to run
   * their jobs.
   */
  public static final String GROUP_NAME = "azkaban";
  /**
   * The Kerberos principal for the job tracker.
   */
  public static final String JT_PRINCIPAL = JTConfig.JT_USER_NAME;
  /**
   * The Kerberos principal for the resource manager.
   */
  public static final String RM_PRINCIPAL = "yarn.resourcemanager.principal";
  // "mapreduce.jobtracker.kerberos.principal";
  public static final String HADOOP_JOB_TRACKER = "mapred.job.tracker";
  public static final String HADOOP_JOB_TRACKER_2 =
      "mapreduce.jobtracker.address";
  public static final String HADOOP_YARN_RM = "yarn.resourcemanager.address";
  /**
   * the key that will be used to set proper signature for each of the hcat token when multiple hcat
   * tokens are required to be fetched.
   */
  public static final String HIVE_TOKEN_SIGNATURE_KEY =
      "hive.metastore.token.signature";
  public static final Text DEFAULT_RENEWER = new Text("azkaban mr tokens");
  public static final String CHOWN = "chown";
  public static final String CHMOD = "chmod";
  // The file permissions assigned to a Delegation token file on fetch
  public static final String TOKEN_FILE_PERMISSIONS = "460";
  private static final String FS_HDFS_IMPL_DISABLE_CACHE =
      "fs.hdfs.impl.disable.cache";
  private static final String OTHER_NAMENODES_TO_GET_TOKEN = "other_namenodes";
  private static final String AZKABAN_KEYTAB_LOCATION = "proxy.keytab.location";
  private static final String AZKABAN_PRINCIPAL = "proxy.user";
  private static final String OBTAIN_JOBHISTORYSERVER_TOKEN =
      "obtain.jobhistoryserver.token";
  private static final String OBTAIN_HIVESERVER2_TOKEN =
          "obtain.hiverserver2.token";
  private final static Logger logger = Logger
      .getLogger(HadoopSecurityManager_H_2_0.class);
  private static final String HIVESERVER2_URL = "hiveserver2.url";
  private static volatile HadoopSecurityManager hsmInstance = null;
  private static URLClassLoader ucl;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private final ExecuteAsUser executeAsUser;
  private final Configuration conf;
  private final ConcurrentMap<String, UserGroupInformation> userUgiMap;
  private UserGroupInformation loginUser = null;
  private String keytabLocation;
  private String keytabPrincipal;
  private boolean shouldProxy = false;
  private boolean securityEnabled = false;

  private HadoopSecurityManager_H_2_0(Props props)
      throws HadoopSecurityManagerException, IOException {
    executeAsUser = new ExecuteAsUser(props.getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));

    // for now, assume the same/compatible native library, the same/compatible
    // hadoop-core jar
    String hadoopHome = props.getString("hadoop.home", null);
    String hadoopConfDir = props.getString("hadoop.conf.dir", null);

    if (hadoopHome == null) {
      hadoopHome = System.getenv("HADOOP_HOME");
    }
    if (hadoopConfDir == null) {
      hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    }

    List<URL> resources = new ArrayList<>();
    URL urlToHadoop = null;
    if (hadoopConfDir != null) {
      urlToHadoop = new File(hadoopConfDir).toURI().toURL();
      HadoopSecurityManager_H_2_0.logger.info("Using hadoop config found in " + urlToHadoop);
      resources.add(urlToHadoop);
    } else if (hadoopHome != null) {
      urlToHadoop = new File(hadoopHome, "conf").toURI().toURL();
      HadoopSecurityManager_H_2_0.logger.info("Using hadoop config found in " + urlToHadoop);
      resources.add(urlToHadoop);
    } else {
      HadoopSecurityManager_H_2_0.logger.info("HADOOP_HOME not set, using default hadoop config.");
    }

    HadoopSecurityManager_H_2_0.ucl = new URLClassLoader(
        resources.toArray(new URL[resources.size()]));

    conf = new Configuration();
    conf.setClassLoader(HadoopSecurityManager_H_2_0.ucl);

    if (props.containsKey(HadoopSecurityManager_H_2_0.FS_HDFS_IMPL_DISABLE_CACHE)) {
      HadoopSecurityManager_H_2_0.logger
          .info("Setting " + HadoopSecurityManager_H_2_0.FS_HDFS_IMPL_DISABLE_CACHE + " to "
              + props.get(HadoopSecurityManager_H_2_0.FS_HDFS_IMPL_DISABLE_CACHE));
      conf.setBoolean(HadoopSecurityManager_H_2_0.FS_HDFS_IMPL_DISABLE_CACHE,
          Boolean.valueOf(props.get(HadoopSecurityManager_H_2_0.FS_HDFS_IMPL_DISABLE_CACHE)));
    }

    HadoopSecurityManager_H_2_0.logger
        .info(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION + ": "
            + conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION));
    HadoopSecurityManager_H_2_0.logger
        .info(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION + ":  "
            + conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION));
    HadoopSecurityManager_H_2_0.logger.info(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY + ": "
        + conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));

    UserGroupInformation.setConfiguration(conf);

    securityEnabled = UserGroupInformation.isSecurityEnabled();
    if (securityEnabled) {
      HadoopSecurityManager_H_2_0.logger.info("The Hadoop cluster has enabled security");
      shouldProxy = true;
      try {

        keytabLocation = props.getString(HadoopSecurityManager_H_2_0.AZKABAN_KEYTAB_LOCATION);
        keytabPrincipal = props.getString(HadoopSecurityManager_H_2_0.AZKABAN_PRINCIPAL);
      } catch (UndefinedPropertyException e) {
        throw new HadoopSecurityManagerException(e.getMessage());
      }

      // try login
      try {
        if (loginUser == null) {
          HadoopSecurityManager_H_2_0.logger.info("No login user. Creating login user");
          HadoopSecurityManager_H_2_0.logger
              .info("Using principal from " + keytabPrincipal + " and "
                  + keytabLocation);
          UserGroupInformation.loginUserFromKeytab(keytabPrincipal,
              keytabLocation);
          loginUser = UserGroupInformation.getLoginUser();
          HadoopSecurityManager_H_2_0.logger.info("Logged in with user " + loginUser);
        } else {
          HadoopSecurityManager_H_2_0.logger.info("loginUser (" + loginUser
              + ") already created, refreshing tgt.");
          loginUser.checkTGTAndReloginFromKeytab();
        }
      } catch (IOException e) {
        throw new HadoopSecurityManagerException(
            "Failed to login with kerberos ", e);
      }

    }

    userUgiMap = new ConcurrentHashMap<>();

    HadoopSecurityManager_H_2_0.logger.info("Hadoop Security Manager initialized");
  }

  public static HadoopSecurityManager getInstance(Props props)
      throws HadoopSecurityManagerException, IOException {
    if (HadoopSecurityManager_H_2_0.hsmInstance == null) {
      synchronized (HadoopSecurityManager_H_2_0.class) {
        if (HadoopSecurityManager_H_2_0.hsmInstance == null) {
          HadoopSecurityManager_H_2_0.logger.info("getting new instance");
          HadoopSecurityManager_H_2_0.hsmInstance = new HadoopSecurityManager_H_2_0(props);
        }
      }
    }

    HadoopSecurityManager_H_2_0.logger.debug("Relogging in from keytab if necessary.");
    HadoopSecurityManager_H_2_0.hsmInstance.reloginFromKeytab();

    return HadoopSecurityManager_H_2_0.hsmInstance;
  }

  /**
   * Create a proxied user based on the explicit user name, taking other parameters necessary from
   * properties file.
   */
  @Override
  public synchronized UserGroupInformation getProxiedUser(String userToProxy)
      throws HadoopSecurityManagerException {

    if (userToProxy == null) {
      throw new HadoopSecurityManagerException("userToProxy can't be null");
    }

    UserGroupInformation ugi = userUgiMap.get(userToProxy);
    if (ugi == null) {
      HadoopSecurityManager_H_2_0.logger.info("proxy user " + userToProxy
          + " not exist. Creating new proxy user");
      if (shouldProxy) {
        try {
          ugi =
              UserGroupInformation.createProxyUser(userToProxy,
                  UserGroupInformation.getLoginUser());
        } catch (IOException e) {
          throw new HadoopSecurityManagerException(
              "Failed to create proxy user", e);
        }
      } else {
        ugi = UserGroupInformation.createRemoteUser(userToProxy);
      }
      userUgiMap.putIfAbsent(userToProxy, ugi);
    }
    return ugi;
  }

  /**
   * Create a proxied user, taking all parameters, including which user to proxy from provided
   * Properties.
   */
  @Override
  public UserGroupInformation getProxiedUser(Props userProp)
      throws HadoopSecurityManagerException {
    String userToProxy = verifySecureProperty(userProp, JobProperties.USER_TO_PROXY);
    UserGroupInformation user = getProxiedUser(userToProxy);
    if (user == null) {
      throw new HadoopSecurityManagerException(
          "Proxy as any user in unsecured grid is not supported!");
    }
    return user;
  }

  public String verifySecureProperty(Props props, String s)
      throws HadoopSecurityManagerException {
    String value = props.getString(s);
    if (value == null) {
      throw new HadoopSecurityManagerException(s + " not set in properties.");
    }
    return value;
  }

  @Override
  public FileSystem getFSAsUser(String user)
      throws HadoopSecurityManagerException {
    FileSystem fs;
    try {
      HadoopSecurityManager_H_2_0.logger.info("Getting file system as " + user);
      UserGroupInformation ugi = getProxiedUser(user);

      if (ugi != null) {
        fs = ugi.doAs(new PrivilegedAction<FileSystem>() {

          @Override
          public FileSystem run() {
            try {
              return FileSystem.get(conf);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      } else {
        fs = FileSystem.get(conf);
      }
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to get FileSystem. ", e);
    }
    return fs;
  }

  public boolean shouldProxy() {
    return shouldProxy;
  }

  private void registerCustomCredential(Props props, Credentials hadoopCred,
      String userToProxy, Logger jobLogger) {
    String credentialClassName = "unknown class";
      try {
        credentialClassName = props
            .getString(Constants.ConfigurationKeys.CUSTOM_CREDENTIAL_NAME);
        HadoopSecurityManager_H_2_0.logger
            .info("custom credential class name: " + credentialClassName);
        Class credentialClass = Class.forName(credentialClassName);

        // The credential class must have a constructor accepting 3 parameters, Credentials,
        // Props, and Logger in order.
        Constructor constructor = credentialClass.getConstructor (new Class[]
            {Credentials.class, Props.class, Logger.class});
        CredentialProvider customCredential = (CredentialProvider) constructor
              .newInstance(hadoopCred, props, jobLogger);
        customCredential.register(userToProxy);

      } catch (Exception e) {
        HadoopSecurityManager_H_2_0.logger
            .error("Encountered error while loading and instantiating "
            + credentialClassName, e);
        throw new IllegalStateException("Encountered error while loading and instantiating "
            + credentialClassName, e);
      }
  }

  @Override
  public boolean isHadoopSecurityEnabled() {
    return securityEnabled;
  }

  /*
   * Gets hadoop tokens for a user to run mapred/pig jobs on a secured cluster
   */
  @Override
  public synchronized void prefetchToken(File tokenFile,
      String userToProxy, Logger logger)
      throws HadoopSecurityManagerException {
    logger.info("Getting hadoop tokens for " + userToProxy);

    UserGroupInformation proxiedUser = getProxiedUser(userToProxy);
    try {
      proxiedUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          getToken(userToProxy);
          return null;
        }

        private void getToken(String userToProxy) throws InterruptedException,
            IOException, HadoopSecurityManagerException {

          FileSystem fs = FileSystem.get(conf);
          // check if we get the correct FS, and most importantly, the conf
          logger.info("Getting DFS token from " + fs.getCanonicalServiceName()
              + fs.getUri());
          Token<?> fsToken = fs.getDelegationToken(userToProxy);
          if (fsToken == null) {
            logger.error("Failed to fetch DFS token for ");
            throw new HadoopSecurityManagerException(
                "Failed to fetch DFS token for " + userToProxy);
          }
          logger.info("Created DFS token.");
          logger.info("Token kind: " + fsToken.getKind());
          logger.info("Token service: " + fsToken.getService());

          JobConf jc = new JobConf(conf);
          JobClient jobClient = new JobClient(jc);
          logger.info("Pre-fetching JT token: Got new JobClient: " + jc);

          Token<DelegationTokenIdentifier> mrdt =
              jobClient.getDelegationToken(new Text("mr token"));
          if (mrdt == null) {
            logger.error("Failed to fetch JT token for ");
            throw new HadoopSecurityManagerException(
                "Failed to fetch JT token for " + userToProxy);
          }
          logger.info("Created JT token.");
          logger.info("Token kind: " + mrdt.getKind());
          logger.info("Token service: " + mrdt.getService());

          jc.getCredentials().addToken(mrdt.getService(), mrdt);
          jc.getCredentials().addToken(fsToken.getService(), fsToken);

          prepareTokenFile(userToProxy, jc.getCredentials(), tokenFile, logger);
          // stash them to cancel after use.
          logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());
        }
      });
    } catch (Exception e) {
      throw new HadoopSecurityManagerException(
          "Failed to get hadoop tokens! " + e.getMessage() + e.getCause());
    }
  }

  private void cancelHiveToken(Token<? extends TokenIdentifier> t,
      String userToProxy) throws HadoopSecurityManagerException {
    try {
      HiveConf hiveConf = new HiveConf();
      IMetaStoreClient hiveClient = createRetryingMetaStoreClient(hiveConf);
      hiveClient.cancelDelegationToken(t.encodeToUrlString());
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to cancel Token. "
          + e.getMessage() + e.getCause(), e);
    }
  }

  @Override
  public void cancelTokens(File tokenFile, String userToProxy, Logger logger)
      throws HadoopSecurityManagerException {
    // nntoken
    Credentials cred = null;
    try {
      cred =
          Credentials.readTokenStorageFile(new Path(tokenFile.toURI()),
              new Configuration());
      for (Token<? extends TokenIdentifier> t : cred.getAllTokens()) {
        logger.info("Got token.");
        logger.info("Token kind: " + t.getKind());
        logger.info("Token service: " + t.getService());

        if (t.getKind().equals(new Text("HIVE_DELEGATION_TOKEN"))) {
          logger.info("Cancelling hive token.");
          cancelHiveToken(t, userToProxy);
        } else if (t.getKind().equals(new Text("RM_DELEGATION_TOKEN"))) {
          logger.info("Ignore cancelling mr job tracker token request.");
        } else if (t.getKind().equals(new Text("HDFS_DELEGATION_TOKEN"))) {
          logger.info("Ignore cancelling namenode token request.");
        } else if (t.getKind().equals(new Text("MR_DELEGATION_TOKEN"))) {
          logger.info("Ignore cancelling jobhistoryserver mr token request.");
        } else {
          logger.info("unknown token type " + t.getKind());
        }
      }
    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to cancel tokens "
          + e.getMessage() + e.getCause(), e);
    }

  }

  /**
   * function to fetch hcat token as per the specified hive configuration and then store the token
   * in to the credential store specified .
   *
   * @param userToProxy String value indicating the name of the user the token will be fetched for.
   * @param hiveConf the configuration based off which the hive client will be initialized.
   * @param logger the logger instance which writes the logging content to the job logs.
   */
  private Token<DelegationTokenIdentifier> fetchHcatToken(String userToProxy,
      HiveConf hiveConf, String tokenSignatureOverwrite, Logger logger)
      throws IOException, MetaException, TException {

    logger.info(HiveConf.ConfVars.METASTOREURIS.varname + ": "
        + hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));

    logger.info(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname + ": "
        + hiveConf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname));

    logger.info(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname + ": "
        + hiveConf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname));

    IMetaStoreClient hiveClient = createRetryingMetaStoreClient(hiveConf);
    String hcatTokenStr =
        hiveClient.getDelegationToken(userToProxy, UserGroupInformation
            .getLoginUser().getShortUserName());
    Token<DelegationTokenIdentifier> hcatToken =
        new Token<>();
    hcatToken.decodeFromUrlString(hcatTokenStr);

    // overwrite the value of the service property of the token if the signature
    // override is specified.
    // If the service field is set, do not overwrite that
    if (hcatToken.getService().getLength() <= 0 && tokenSignatureOverwrite != null
        && tokenSignatureOverwrite.trim().length() > 0) {
      hcatToken.setService(new Text(tokenSignatureOverwrite.trim()
          .toLowerCase()));

      logger.info(HadoopSecurityManager_H_2_0.HIVE_TOKEN_SIGNATURE_KEY + ":"
          + (tokenSignatureOverwrite == null ? "" : tokenSignatureOverwrite));
    }

    logger.info("Created hive metastore token.");
    logger.info("Token kind: " + hcatToken.getKind());
    logger.info("Token service: " + hcatToken.getService());
    return hcatToken;
  }

  /*
   * Gets hadoop tokens for a user to run mapred/hive jobs on a secured cluster
   */
  @Override
  public synchronized void prefetchToken(File tokenFile,
      Props props, Logger logger)
      throws HadoopSecurityManagerException {

    String userToProxy = props.getString(JobProperties.USER_TO_PROXY);

    logger.info("Getting hadoop tokens based on props for " + userToProxy);

    Credentials cred = new Credentials();

    if (props.getBoolean(HadoopSecurityManager.OBTAIN_HCAT_TOKEN, false)) {
      try {

        // first we fetch and save the default hcat token.
        logger.info("Pre-fetching default Hive MetaStore token from hive");

        HiveConf hiveConf = new HiveConf();
        Token<DelegationTokenIdentifier> hcatToken =
            fetchHcatToken(userToProxy, hiveConf, null, logger);

        cred.addToken(hcatToken.getService(), hcatToken);

        // Added support for extra_hcat_clusters
        List<String> extraHcatClusters = props.getStringListFromCluster(EXTRA_HCAT_CLUSTERS);
        if (Collections.EMPTY_LIST != extraHcatClusters) {
          logger.info("Need to pre-fetch extra metaStore tokens from extra hive clusters.");

          // start to process the user inputs.
          for (String thriftUrls : extraHcatClusters) {
            logger.info("Pre-fetching metaStore token from cluster : " + thriftUrls);

            hiveConf = new HiveConf();
            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUrls);
            hcatToken = fetchHcatToken(userToProxy, hiveConf, thriftUrls, logger);
            cred.addToken(hcatToken.getService(), hcatToken);
          }
        } else {
          // Only if EXTRA_HCAT_CLUSTERS
          List<String> extraHcatLocations =
              props.getStringList(EXTRA_HCAT_LOCATION);
          if (Collections.EMPTY_LIST != extraHcatLocations) {
            logger.info("Need to pre-fetch extra metaStore tokens from hive.");

            // start to process the user inputs.
            for (String thriftUrl : extraHcatLocations) {
              logger.info("Pre-fetching metaStore token from : " + thriftUrl);

              hiveConf = new HiveConf();
              hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUrl);
              hcatToken =
                  fetchHcatToken(userToProxy, hiveConf, thriftUrl, logger);
              cred.addToken(hcatToken.getService(), hcatToken);
            }
          }
        }

      } catch (Throwable t) {
        String message =
            "Failed to get hive metastore token." + t.getMessage()
                + t.getCause();
        logger.error(message, t);
        throw new HadoopSecurityManagerException(message);
      }
    }

    if (props.getBoolean(HadoopSecurityManager_H_2_0.OBTAIN_JOBHISTORYSERVER_TOKEN, false)) {
      YarnRPC rpc = YarnRPC.create(conf);
      String serviceAddr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS);

      logger.debug("Connecting to HistoryServer at: " + serviceAddr);
      HSClientProtocol hsProxy =
          (HSClientProtocol) rpc.getProxy(HSClientProtocol.class,
              NetUtils.createSocketAddr(serviceAddr), conf);
      logger.info("Pre-fetching JH token from job history server");

      Token<?> jhsdt = null;
      try {
        jhsdt = getDelegationTokenFromHS(hsProxy);
      } catch (Exception e) {
        logger.error("Failed to fetch JH token", e);
        throw new HadoopSecurityManagerException(
            "Failed to fetch JH token for " + userToProxy);
      }

      if (jhsdt == null) {
        logger.error("getDelegationTokenFromHS() returned null");
        throw new HadoopSecurityManagerException(
            "Unable to fetch JH token for " + userToProxy);
      }

      logger.info("Created JH token.");
      logger.info("Token kind: " + jhsdt.getKind());
      logger.info("Token service: " + jhsdt.getService());

      cred.addToken(jhsdt.getService(), jhsdt);
    }

    if (props.getBoolean(HadoopSecurityManager_H_2_0.OBTAIN_HIVESERVER2_TOKEN, false)) {
      Connection conn = null;
      Token<DelegationTokenIdentifier> hive2Token = null;
      try {
        HiveConf hiveConf = new HiveConf();
        String principal = hiveConf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname);
        logger.info(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname + ":" + principal);
        String url =
            props.get(HadoopSecurityManager_H_2_0.HIVESERVER2_URL) + ";principal=" + principal;
        logger.info("final url for hiveserver2:" + url);
        conn = DriverManager.getConnection(url);
        String tokenStr = ((HiveConnection) conn).getDelegationToken(userToProxy, principal);
        hive2Token = new Token<>();
        hive2Token.decodeFromUrlString(tokenStr);

        cred.addToken(hive2Token.getService(), hive2Token);
      } catch (Exception e) {
        logger.error("Failed to get hiveserver2 token", e);
        throw new HadoopSecurityManagerException(
                "Failed to get hiveserver2 token for " + userToProxy);
      } finally {
        if (conn != null) {
          try {
            conn.close();
          } catch (SQLException e) {
            logger.error("could not close connection", e);
          }
        }
      }

      logger.info("Created hive server2 token.");
      logger.info("Token kind: " + hive2Token.getKind());
      logger.info("Token service: " + hive2Token.getService());
    }

    try {
      getProxiedUser(userToProxy).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          getToken(userToProxy);
          return null;
        }

        private void getToken(String userToProxy) throws InterruptedException,
            IOException, HadoopSecurityManagerException {
          logger.info("Here is the props for " + HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN + ": "
              + props.getBoolean(HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN));

          // Register user secrets by custom credential Object
          if (props.getBoolean(JobProperties.ENABLE_JOB_SSL, false)) {
            registerCustomCredential(props, cred, userToProxy, logger);
          }

          if (props.getBoolean(HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN, false)) {
            FileSystem fs = FileSystem.get(conf);
            // check if we get the correct FS, and most importantly, the
            // conf
            logger.info("Getting DFS token from " + fs.getUri());
            Token<?> fsToken =
                fs.getDelegationToken(getMRTokenRenewerInternal(new JobConf())
                    .toString());
            if (fsToken == null) {
              logger.error("Failed to fetch DFS token for ");
              throw new HadoopSecurityManagerException(
                  "Failed to fetch DFS token for " + userToProxy);
            }
            logger.info("Created DFS token.");
            logger.info("Token kind: " + fsToken.getKind());
            logger.info("Token service: " + fsToken.getService());

            cred.addToken(fsToken.getService(), fsToken);

            // getting additional name nodes tokens
            String otherNamenodes = props.get(
                HadoopSecurityManager_H_2_0.OTHER_NAMENODES_TO_GET_TOKEN);
            if ((otherNamenodes != null) && (otherNamenodes.length() > 0)) {
              logger.info(
                  HadoopSecurityManager_H_2_0.OTHER_NAMENODES_TO_GET_TOKEN + ": '" + otherNamenodes
                  + "'");
              String[] nameNodeArr = otherNamenodes.split(",");
              Path[] ps = new Path[nameNodeArr.length];
              for (int i = 0; i < ps.length; i++) {
                ps[i] = new Path(nameNodeArr[i].trim());
              }
              TokenCache.obtainTokensForNamenodes(cred, ps, conf);
              logger.info("Successfully fetched tokens for: " + otherNamenodes);
            } else {
              logger.info(
                  HadoopSecurityManager_H_2_0.OTHER_NAMENODES_TO_GET_TOKEN + " was not configured");
            }
          }

          if (props.getBoolean(HadoopSecurityManager.OBTAIN_JOBTRACKER_TOKEN, false)) {
            JobConf jobConf = new JobConf();
            JobClient jobClient = new JobClient(jobConf);
            logger.info("Pre-fetching JT token from JobTracker");

            Token<DelegationTokenIdentifier> mrdt =
                jobClient
                    .getDelegationToken(getMRTokenRenewerInternal(jobConf));
            if (mrdt == null) {
              logger.error("Failed to fetch JT token");
              throw new HadoopSecurityManagerException(
                  "Failed to fetch JT token for " + userToProxy);
            }
            logger.info("Created JT token.");
            logger.info("Token kind: " + mrdt.getKind());
            logger.info("Token service: " + mrdt.getService());
            cred.addToken(mrdt.getService(), mrdt);
          }

        }
      });

      prepareTokenFile(userToProxy, cred, tokenFile, logger);
      // stash them to cancel after use.

      logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());

    } catch (Exception e) {
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + e.getMessage() + e.getCause(), e);
    } catch (Throwable t) {
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + t.getMessage() + t.getCause(), t);
    }

  }

  /**
   * Prepare token file. Writes credentials to a token file and sets appropriate permissions to keep
   * the file secure
   *
   * @param user user to be proxied
   * @param credentials Credentials to be written to file
   * @param tokenFile file to be written
   * @param logger logger to use
   * @throws IOException If there are issues in reading / updating the token file
   */
  private void prepareTokenFile(String user,
      Credentials credentials,
      File tokenFile,
      Logger logger) throws IOException {
    writeCredentialsToFile(credentials, tokenFile, logger);
    try {
      assignPermissions(user, tokenFile, logger);
    } catch (IOException e) {
      // On any error managing token file. delete the file
      tokenFile.delete();
      throw e;
    }
  }

  private void writeCredentialsToFile(Credentials credentials, File tokenFile,
      Logger logger)
      throws IOException {
    FileOutputStream fos = null;
    DataOutputStream dos = null;
    try {
      fos = new FileOutputStream(tokenFile);
      dos = new DataOutputStream(fos);
      credentials.writeTokenStorageToStream(dos);
    } finally {
      if (dos != null) {
        try {
          dos.close();
        } catch (Throwable t) {
          // best effort
          logger.error("encountered exception while closing DataOutputStream of the tokenFile", t);
        }
      }
      if (fos != null) {
        fos.close();
      }
    }
  }

  /**
   * Uses execute-as-user binary to reassign file permissions to be readable only by that user.
   *
   * Step 1. Set file permissions to 460. Readable to self and readable / writable azkaban group
   * Step 2. Set user as owner of file.
   *
   * @param user user to be proxied
   * @param tokenFile file to be written
   * @param logger logger to use
   */
  private void assignPermissions(String user, File tokenFile, Logger logger)
      throws IOException {
    List<String> changePermissionsCommand = Arrays.asList(
        HadoopSecurityManager_H_2_0.CHMOD, HadoopSecurityManager_H_2_0.TOKEN_FILE_PERMISSIONS,
        tokenFile.getAbsolutePath()
    );
    int result = executeAsUser
        .execute(System.getProperty("user.name"), changePermissionsCommand);
    if (result != 0) {
      throw new IOException("Unable to modify permissions. User: " + user);
    }

    List<String> changeOwnershipCommand = Arrays.asList(
        HadoopSecurityManager_H_2_0.CHOWN, user + ":" + HadoopSecurityManager_H_2_0.GROUP_NAME,
        tokenFile.getAbsolutePath()
    );
    result = executeAsUser.execute("root", changeOwnershipCommand);
    if (result != 0) {
      throw new IOException("Unable to set ownership. User: " + user);
    }
  }

  private Text getMRTokenRenewerInternal(JobConf jobConf) throws IOException {
    // Taken from Oozie
    //
    // Getting renewer correctly for JT principal also though JT in hadoop
    // 1.x does not have
    // support for renewing/cancelling tokens
    String servicePrincipal =
        jobConf.get(HadoopSecurityManager_H_2_0.RM_PRINCIPAL, jobConf.get(
            HadoopSecurityManager_H_2_0.JT_PRINCIPAL));
    Text renewer;
    if (servicePrincipal != null) {
      String target =
          jobConf.get(HadoopSecurityManager_H_2_0.HADOOP_YARN_RM, jobConf.get(
              HadoopSecurityManager_H_2_0.HADOOP_JOB_TRACKER_2));
      if (target == null) {
        target = jobConf.get(HadoopSecurityManager_H_2_0.HADOOP_JOB_TRACKER);
      }

      String addr = NetUtils.createSocketAddr(target).getHostName();
      renewer =
          new Text(SecurityUtil.getServerPrincipal(servicePrincipal, addr));
    } else {
      // No security
      renewer = HadoopSecurityManager_H_2_0.DEFAULT_RENEWER;
    }

    return renewer;
  }

  private Token<?> getDelegationTokenFromHS(HSClientProtocol hsProxy)
      throws IOException, InterruptedException {
    GetDelegationTokenRequest request =
        recordFactory.newRecordInstance(GetDelegationTokenRequest.class);
    request.setRenewer(Master.getMasterPrincipal(conf));
    org.apache.hadoop.yarn.api.records.Token mrDelegationToken;
    mrDelegationToken =
        hsProxy.getDelegationToken(request).getDelegationToken();
    return ConverterUtils.convertFromYarn(mrDelegationToken,
        hsProxy.getConnectAddress());
  }

  /**
   * Method to create a metastore client that retries on failures
   */
  private IMetaStoreClient createRetryingMetaStoreClient(HiveConf hiveConf)
      throws MetaException {
    // Custom hook-loader to return a HiveMetaHook if the table is configured with a custom storage handler
    HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
      @Override
      public HiveMetaHook getHook(Table tbl) throws MetaException {
        if (tbl == null) {
          return null;
        }

        try {
          HiveStorageHandler storageHandler =
              HiveUtils.getStorageHandler(hiveConf, tbl.getParameters().get(META_TABLE_STORAGE));
          return storageHandler == null ? null : storageHandler.getMetaHook();
        } catch (HiveException e) {
          HadoopSecurityManager_H_2_0.logger.error(e.toString());
          throw new MetaException("Failed to get storage handler: " + e);
        }
      }
    };

    return RetryingMetaStoreClient
        .getProxy(hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
  }
}
