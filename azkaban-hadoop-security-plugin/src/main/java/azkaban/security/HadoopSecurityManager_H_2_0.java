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

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_HOST_NAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER;
import static azkaban.Constants.JobProperties.EXTRA_HCAT_CLUSTERS;
import static azkaban.Constants.JobProperties.EXTRA_HCAT_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

import azkaban.Constants;
import azkaban.Constants.FlowProperties;
import azkaban.Constants.JobProperties;
import azkaban.ServiceProvider;
import azkaban.executor.KeyStoreManager;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.ExecuteAsUser;
import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
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
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class HadoopSecurityManager_H_2_0 extends HadoopSecurityManager {

  // Use azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER instead
  @Deprecated
  public static final String NATIVE_LIB_FOLDER = "azkaban.native.lib";
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
  private static final String FS_LOCAL_IMPL_DISABLE_CACHE =
      "fs.file.impl.disable.cache";
  // Some hadoop clusters have failover name nodes.
  private static final String FS_FAILOVER_IMPL_DISABLE_CACHE =
          "fs.failover.impl.disable.cache";
  private static final String IMPL_DISABLE_CACHE_SUFFIX =
          ".impl.disable.cache";
  private static final String OTHER_NAMENODES_TO_GET_TOKEN = "other_namenodes";
  private static final String AZKABAN_KEYTAB_LOCATION = "proxy.keytab.location";
  private static final String AZKABAN_PRINCIPAL = "proxy.user";
  private static final String OBTAIN_JOBHISTORYSERVER_TOKEN =
      "obtain.jobhistoryserver.token";
  private final static Logger logger = Logger
      .getLogger(HadoopSecurityManager_H_2_0.class);

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private final ExecuteAsUser executeAsUser;
  private final Configuration conf;
  private final ConcurrentMap<String, UserGroupInformation> userUgiMap;
  private UserGroupInformation loginUser = null;
  private String keytabLocation;
  private String keytabPrincipal;
  private boolean shouldProxy = false;
  private boolean securityEnabled = false;

  public HadoopSecurityManager_H_2_0(final Props props)
      throws HadoopSecurityManagerException, IOException {
    this.executeAsUser = new ExecuteAsUser(props.getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));
    this.conf = new Configuration();

    // Disable yyFileSystem Cache for HadoopSecurityManager
    disableFSCache();

    logger.info(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION + ": "
        + this.conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION));
    logger.info(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION + ":  "
        + this.conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION));
    logger.info(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY + ": "
        + this.conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY));

    UserGroupInformation.setConfiguration(this.conf);

    this.securityEnabled = UserGroupInformation.isSecurityEnabled();
    if (this.securityEnabled) {
      logger.info("The Hadoop cluster has enabled security");
      this.shouldProxy = true;
      try {

        this.keytabLocation = props.getString(AZKABAN_KEYTAB_LOCATION);
        this.keytabPrincipal = props.getString(AZKABAN_PRINCIPAL);
      } catch (final UndefinedPropertyException e) {
        throw new HadoopSecurityManagerException(e.getMessage());
      }

      // try login
      try {
        if (this.loginUser == null) {
          logger.info("No login user. Creating login user");
          logger.info("Using principal from " + this.keytabPrincipal + " and "
              + this.keytabLocation);
          UserGroupInformation.loginUserFromKeytab(this.keytabPrincipal,
              this.keytabLocation);
          this.loginUser = UserGroupInformation.getLoginUser();
          logger.info("Logged in with user " + this.loginUser);
        } else {
          logger.info("loginUser (" + this.loginUser
              + ") already created, refreshing tgt.");
          this.loginUser.checkTGTAndReloginFromKeytab();
        }
      } catch (final IOException e) {
        throw new HadoopSecurityManagerException(
            "Failed to login with kerberos ", e);
      }

    }

    this.userUgiMap = new ConcurrentHashMap<>();

    logger.info("Hadoop Security Manager initialized");
  }

  // Disable yyFileSystem Cache for HadoopSecurityManager
  private void disableFSCache() {
    this.conf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);
    this.conf.setBoolean(FS_FAILOVER_IMPL_DISABLE_CACHE, true);
    this.conf.setBoolean(FS_LOCAL_IMPL_DISABLE_CACHE, true);
    // Get the default scheme
    final String defaultFS = conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY);
    if (defaultFS == null) {
      return;
    }
    final String scheme = new Path(defaultFS).toUri().getScheme();
    if (scheme == null) {
      return;
    }
    // Construct the property name
    final String FS_DEFAULT_IMPL_DISABLE_CACHE =
            "fs." + scheme + IMPL_DISABLE_CACHE_SUFFIX;
    this.conf.setBoolean(FS_DEFAULT_IMPL_DISABLE_CACHE, true);
    logger.info("Disable cache for scheme " + FS_DEFAULT_IMPL_DISABLE_CACHE);

  }

  /**
   * Create a proxied user based on the explicit user name, taking other parameters necessary from
   * properties file.
   */
  @Override
  public synchronized UserGroupInformation getProxiedUser(final String userToProxy)
      throws HadoopSecurityManagerException {

    if (userToProxy == null) {
      throw new HadoopSecurityManagerException("userToProxy can't be null");
    }

    UserGroupInformation ugi = this.userUgiMap.get(userToProxy);
    if (ugi == null) {
      logger.info("proxy user " + userToProxy
          + " not exist. Creating new proxy user");
      if (this.shouldProxy) {
        try {
          ugi =
              UserGroupInformation.createProxyUser(userToProxy,
                  UserGroupInformation.getLoginUser());
        } catch (final IOException e) {
          throw new HadoopSecurityManagerException(
              "Failed to create proxy user", e);
        }
      } else {
        ugi = UserGroupInformation.createRemoteUser(userToProxy);
      }
      this.userUgiMap.putIfAbsent(userToProxy, ugi);
    }
    return ugi;
  }

  /**
   * Create a proxied user, taking all parameters, including which user to proxy from provided
   * Properties.
   */
  @Override
  public UserGroupInformation getProxiedUser(final Props userProp)
      throws HadoopSecurityManagerException {
    final String userToProxy = verifySecureProperty(userProp, JobProperties.USER_TO_PROXY);
    final UserGroupInformation user = getProxiedUser(userToProxy);
    if (user == null) {
      throw new HadoopSecurityManagerException(
          "Proxy as any user in unsecured grid is not supported!");
    }
    return user;
  }

  public String verifySecureProperty(final Props props, final String s)
      throws HadoopSecurityManagerException {
    final String value = props.getString(s);
    if (value == null) {
      throw new HadoopSecurityManagerException(s + " not set in properties.");
    }
    return value;
  }

  @Override
  public FileSystem getFSAsUser(final String user)
      throws HadoopSecurityManagerException {
    final FileSystem fs;
    try {
      logger.info("Getting file system as " + user);
      final UserGroupInformation ugi = getProxiedUser(user);

      if (ugi != null) {
        fs = ugi.doAs(new PrivilegedAction<FileSystem>() {

          @Override
          public FileSystem run() {
            try {
              return FileSystem.get(HadoopSecurityManager_H_2_0.this.conf);
            } catch (final IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      } else {
        fs = FileSystem.get(this.conf);
      }
    } catch (final Exception e) {
      throw new HadoopSecurityManagerException("Failed to get FileSystem. ", e);
    }
    return fs;
  }

  public boolean shouldProxy() {
    return this.shouldProxy;
  }

  private CredentialProviderWithKeyStore getCustomCredentialProvider(final Props props, final Credentials hadoopCred,
      final Logger jobLogger, final String customCredentialProviderName) {
    String credentialClassName = "unknown class";
    try {
      credentialClassName = props.getString(customCredentialProviderName);
      logger.info("custom credential class name: " + credentialClassName);
      final Class credentialClass = Class.forName(credentialClassName);

      // The credential class must have a constructor accepting 3 parameters, Credentials,
      // Props, and Logger in order.
      final Constructor constructor = credentialClass.getConstructor(Credentials.class, Props.class, Logger.class);
      final CredentialProviderWithKeyStore customCredential = (CredentialProviderWithKeyStore) constructor
              .newInstance(hadoopCred, props, jobLogger);
      return customCredential;
    } catch (final Exception e) {
      logger.error("Encountered error while loading and instantiating "
              + credentialClassName, e);
      throw new IllegalStateException("Encountered error while loading and instantiating "
              + credentialClassName, e);
    }
  }

  private void registerCustomCredential(final Props props, final Credentials hadoopCred,
      final String userToProxy, final Logger jobLogger, final String customCredentialProviderName) {
    final CredentialProviderWithKeyStore customCredential = getCustomCredentialProvider(
            props, hadoopCred, jobLogger, customCredentialProviderName);
    final KeyStore keyStore = KeyStoreManager.getInstance().getKeyStore();
    if (keyStore != null) {
      // Containerized Execution.
      customCredential.setKeyStore(keyStore);
    }
    customCredential.register(userToProxy);
  }

  @Override
  public KeyStore getKeyStore(final Props props) {
    logger.info("Prefetching KeyStore for the flow");
    final Credentials cred = new Credentials();
    final CredentialProviderWithKeyStore customCredential = getCustomCredentialProvider(
            props, cred, logger, Constants.ConfigurationKeys.CUSTOM_CREDENTIAL_NAME);
    final KeyStore keyStore = customCredential.getKeyStore();
    KeyStoreManager.getInstance().setKeyStore(keyStore);
    return keyStore;
  }

  @Override
  public boolean isHadoopSecurityEnabled() {
    return this.securityEnabled;
  }


  private void cancelHiveToken(final Token<? extends TokenIdentifier> t,
      final String userToProxy) throws HadoopSecurityManagerException {
    try {
      final HiveConf hiveConf = new HiveConf();
      final IMetaStoreClient hiveClient = createRetryingMetaStoreClient(hiveConf);
      hiveClient.cancelDelegationToken(t.encodeToUrlString());
    } catch (final Exception e) {
      throw new HadoopSecurityManagerException("Failed to cancel Token. "
          + e.getMessage() + e.getCause(), e);
    }
  }

  @Override
  public void cancelTokens(final File tokenFile, final String userToProxy, final Logger logger)
      throws HadoopSecurityManagerException {
    // nntoken
    Credentials cred = null;
    try {
      cred =
          Credentials.readTokenStorageFile(new Path(tokenFile.toURI()),
              this.conf);
      for (final Token<? extends TokenIdentifier> t : cred.getAllTokens()) {
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
    } catch (final Exception e) {
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
  private Token<DelegationTokenIdentifier> fetchHcatToken(final String userToProxy,
      final HiveConf hiveConf, final String tokenSignatureOverwrite, final Logger logger)
      throws IOException, MetaException, TException {

    logger.info(HiveConf.ConfVars.METASTOREURIS.varname + ": "
        + hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));

    logger.info(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname + ": "
        + hiveConf.get(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname));

    logger.info(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname + ": "
        + hiveConf.get(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname));

    final IMetaStoreClient hiveClient = createRetryingMetaStoreClient(hiveConf);
    final String hcatTokenStr =
        hiveClient.getDelegationToken(userToProxy, UserGroupInformation
            .getLoginUser().getShortUserName());
    final Token<DelegationTokenIdentifier> hcatToken =
        new Token<>();
    hcatToken.decodeFromUrlString(hcatTokenStr);

    // overwrite the value of the service property of the token if the signature
    // override is specified.
    // If the service field is set, do not overwrite that
    if (hcatToken.getService().getLength() <= 0 && tokenSignatureOverwrite != null
        && tokenSignatureOverwrite.trim().length() > 0) {
      hcatToken.setService(new Text(tokenSignatureOverwrite.trim()
          .toLowerCase()));

      logger.info(HIVE_TOKEN_SIGNATURE_KEY + ":" + tokenSignatureOverwrite);
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
  public void prefetchToken(final File tokenFile, final Props props, final Logger logger)
      throws HadoopSecurityManagerException {
    final String userToProxy = props.getString(JobProperties.USER_TO_PROXY);

    doPrefetch(tokenFile, props, logger, userToProxy);
  }

  /*
   * Create a suffix for Kerberos principal, the format is,
   * az_<host name>_<execution id><DOMAIN_NAME>
   */
  private String kerberosSuffix(final Props props) {
    // AZKABAN_SERVER_HOST_NAME is not set in Props here, get it from another instance of Props.
    final String host = ServiceProvider.SERVICE_PROVIDER.getInstance(Props.class)
        .getString(AZKABAN_SERVER_HOST_NAME, "unknown");
    final StringBuilder builder = new StringBuilder("az_");
    builder.append(host);
    builder.append("_");
    builder.append(props.getString(FlowProperties.AZKABAN_FLOW_EXEC_ID));
    builder.append(props.getString(HadoopSecurityManager.DOMAIN_NAME));
    return builder.toString();
  }
  private void doPrefetch(final File tokenFile, final Props props, final Logger logger,
      final String userToProxy) throws HadoopSecurityManagerException {
    // Create suffix to be added to kerberos principal
    final String suffix =
        (null != props.getString(HadoopSecurityManager.DOMAIN_NAME, null)) ?
            "/" + kerberosSuffix(props): "";


    final String userToProxyFQN = userToProxy + suffix;
    logger.info("Getting hadoop tokens based on props for " + userToProxyFQN);

    final Credentials cred = new Credentials();
    fetchMetaStoreToken(props, logger, userToProxyFQN, cred);

    try {
      fetchJHSToken(props, logger, userToProxyFQN, cred);

      getProxiedUser(userToProxyFQN).doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          getToken(userToProxyFQN, userToProxy);
          return null;
        }

        private void getToken(final String userToProxyFQN, final String userToProxy)
            throws InterruptedException, IOException, HadoopSecurityManagerException {
          logger.info("Here is the props for " + HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN +
              ": " + props.getBoolean(HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN));

          // Register user secrets by custom credential Object
          if (props.getBoolean(JobProperties.ENABLE_JOB_SSL, false)) {
            registerCustomCredential(props, cred, userToProxy, logger,
                Constants.ConfigurationKeys.CUSTOM_CREDENTIAL_NAME);
          }

          // Register oauth tokens by custom oauth credential provider
          if (props.getBoolean(JobProperties.ENABLE_OAUTH, false)) {
            registerCustomCredential(props, cred, userToProxy, logger,
                Constants.ConfigurationKeys.OAUTH_CREDENTIAL_NAME);
          }

          fetchNameNodeToken(userToProxyFQN, props, logger, cred);

          fetchJobTrackerToken(userToProxyFQN, props, logger, cred);

        }
      });

      logger.info("Preparing token file " + tokenFile.getAbsolutePath());
      prepareTokenFile(userToProxy, cred, tokenFile, logger,
          props.getString(Constants.ConfigurationKeys.SECURITY_USER_GROUP, "azkaban"));
      // stash them to cancel after use.

      logger.info("Tokens loaded in " + tokenFile.getAbsolutePath());

    } catch (final Exception e) {
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + e.getMessage() + e.getCause(), e);
    } catch (final Throwable t) {
      throw new HadoopSecurityManagerException("Failed to get hadoop tokens! "
          + t.getMessage() + t.getCause(), t);
    }
  }

  private void fetchJobTrackerToken(final String userToProxy, final Props props,
      final Logger logger, final Credentials cred)
      throws IOException, InterruptedException, HadoopSecurityManagerException {
    if (props.getBoolean(HadoopSecurityManager.OBTAIN_JOBTRACKER_TOKEN, false)) {
      final JobConf jobConf = new JobConf();
      final JobClient jobClient = new JobClient(jobConf);
      logger.info("Pre-fetching JT token from JobTracker");
      Token<DelegationTokenIdentifier> mrdt;
      try {
        mrdt = jobClient.getDelegationToken(getMRTokenRenewerInternal(jobConf));
      } finally {
        jobClient.close();
      }

      if (mrdt == null) {
        logger.error("Failed to fetch JT token");
        throw new HadoopSecurityManagerException(
            "Failed to fetch JT token for " + userToProxy);
      }

      logger.info(String.format("JT token pre-fetched, token kind: %s, token service: %s",
          mrdt.getKind(), mrdt.getService()));
      cred.addToken(mrdt.getService(), mrdt);
    }
  }

  private void fetchNameNodeToken(final String userToProxy, final Props props, final Logger logger,
          final Credentials cred) throws IOException, HadoopSecurityManagerException {
    if (props.getBoolean(HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN, false)) {
      final String renewer = getMRTokenRenewerInternal(new JobConf()).toString();

      // Get the tokens name node
      fetchNameNodeTokenInternal(renewer, cred, userToProxy, null);

      // getting additional name nodes tokens
      final String otherNamenodes = props.get(
          HadoopSecurityManager_H_2_0.OTHER_NAMENODES_TO_GET_TOKEN);
      if ((otherNamenodes != null) && (otherNamenodes.length() > 0)) {
        logger.info("Fetching token(s) for other namenode(s): " + otherNamenodes);
        final String[] nameNodeArr = otherNamenodes.split(",");
        for (String nameNode : nameNodeArr) {
          fetchNameNodeTokenInternal(renewer, cred, userToProxy, new Path(nameNode.trim()).toUri());
        }
      }
      logger.info("Successfully fetched tokens for: " + otherNamenodes);
    } else {
      logger.info(
              HadoopSecurityManager_H_2_0.OTHER_NAMENODES_TO_GET_TOKEN + " was not configured");
    }
  }

  /**
   * fetchNameNodeInternal - With modified UGI which is of the format,
   * <userToProxy>/az_<host name>_<exec_id>
   * Due to this change, the FileSystem cache creates an entry per execution instead of an entry
   * per proxy user. This could blow up the cache very quickly on a busy Executor and cause OOM.
   * To make this worse, the entry in Cache is never used as it is specfic to an execution.
   * To avoid this, the FileSystem Cache should be disabled before calling this method.
   *
   * @param renewer
   * @param cred
   * @param userToProxy
   * @param uri
   * @throws IOException
   * @throws HadoopSecurityManagerException
   */

  private void fetchNameNodeTokenInternal(final String renewer, final Credentials cred,
          final String userToProxy, final URI uri)
          throws IOException, HadoopSecurityManagerException {
    FileSystem fs = null;
    try {
      // Use FileSystem.get() instead of newInstance() to ensure cache is not used.
      // .get() method checks if cache is enabled or not, newInstance() does not.
      if (uri == null) {
        fs = FileSystem.get(conf);
      } else {
        fs = FileSystem.get(uri, conf);
      }
      // check if we get the correct FS, and most importantly, the conf
      logger.info("Getting DFS token from " + fs.getUri());
      try {
        final Token<?>[] fsTokens = fs.addDelegationTokens(renewer, cred);
        for (int i = 0; i < fsTokens.length; i++) {
          final Token<?> fsToken = fsTokens[i];
          logger.info(String.format(
                  "DFS token from namenode pre-fetched, token kind: %s, token service: %s",
                  fsToken.getKind(), fsToken.getService()));
        }
      } catch (Exception e) {
        logger.error("Failed to fetch DFS token for " + userToProxy);
        throw new HadoopSecurityManagerException(
                "Failed to fetch DFS token for " + userToProxy);
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }

  private void fetchJHSToken(
      final Props props, final Logger logger, final String userToProxy, final Credentials cred)
      throws HadoopSecurityManagerException, IOException {
    if (props.getBoolean(OBTAIN_JOBHISTORYSERVER_TOKEN, false)) {
      logger.info("Pre-fetching JH token from job history server");

      final YarnRPC rpc = YarnRPC.create(this.conf);
      final String serviceAddr = this.conf.get(JHAdminConfig.MR_HISTORY_ADDRESS);

      logger.info("Connecting to HistoryServer at: " + serviceAddr);
      final HSClientProtocol hsProxy =
          (HSClientProtocol) rpc.getProxy(HSClientProtocol.class,
              NetUtils.createSocketAddr(serviceAddr), this.conf);

      Token<?> jhsdt = null;
      try {
        jhsdt = getDelegationTokenFromHS(hsProxy);
      } catch (final Exception e) {
        logger.error("Failed to fetch JH token", e);
        throw new HadoopSecurityManagerException(
            "Failed to fetch JH token for " + userToProxy);
      }

      if (hsProxy instanceof Closeable) {
        // HSClientProtocol is not closable, but its only implementation, HSClientProtocolPBClientImpl, is
        ((Closeable) hsProxy).close();
      }

      if (jhsdt == null) {
        logger.error("getDelegationTokenFromHS() returned null");
        throw new HadoopSecurityManagerException(
            "Unable to fetch JH token for " + userToProxy);
      }

      logger.info(String
          .format("JH token from job history server pre-fetched, token Kind: %s, token service: %s",
              jhsdt.getKind(), jhsdt.getService()));

      cred.addToken(jhsdt.getService(), jhsdt);
    }
  }

  private void fetchMetaStoreToken(final Props props, final Logger logger, final String userToProxy,
      final Credentials cred)
      throws HadoopSecurityManagerException {
    if (props.getBoolean(HadoopSecurityManager.OBTAIN_HCAT_TOKEN, false)) {
      try {

        // first we fetch and save the default hcat token.
        logger.info("Pre-fetching default hive metastore token from hive");

        HiveConf hiveConf = new HiveConf();
        Token<DelegationTokenIdentifier> hcatToken =
            fetchHcatToken(userToProxy, hiveConf, null, logger);

        cred.addToken(hcatToken.getService(), hcatToken);

        // Added support for extra_hcat_clusters
        final List<String> extraHcatClusters = props.getStringListFromCluster(EXTRA_HCAT_CLUSTERS);
        if (Collections.EMPTY_LIST != extraHcatClusters) {
          logger.info("Need to pre-fetch extra metastore tokens from extra hive clusters.");

          // start to process the user inputs.
          for (final String thriftUrls : extraHcatClusters) {
            logger.info("Pre-fetching metastore token from cluster : " + thriftUrls);

            hiveConf = new HiveConf();
            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUrls);
            hcatToken = fetchHcatToken(userToProxy, hiveConf, thriftUrls, logger);
            cred.addToken(hcatToken.getService(), hcatToken);
          }
        } else {
          // Only if EXTRA_HCAT_CLUSTERS
          final List<String> extraHcatLocations =
              props.getStringList(EXTRA_HCAT_LOCATION);
          if (Collections.EMPTY_LIST != extraHcatLocations) {
            logger.info("Need to pre-fetch extra metastore tokens from hive.");

            // start to process the user inputs.
            for (final String thriftUrl : extraHcatLocations) {
              logger.info("Pre-fetching metastore token from : " + thriftUrl);

              hiveConf = new HiveConf();
              hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUrl);
              hcatToken =
                  fetchHcatToken(userToProxy, hiveConf, thriftUrl, logger);
              cred.addToken(hcatToken.getService(), hcatToken);
            }
          }
        }

        logger.info("Hive metastore token(s) prefetched");

      } catch (final Throwable t) {
        final String message =
            "Failed to get hive metastore token." + t.getMessage()
                + t.getCause();
        logger.error(message, t);
        throw new HadoopSecurityManagerException(message);
      }
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
   * @param group user group to own the token file
   * @throws IOException If there are issues in reading / updating the token file
   */
  private void prepareTokenFile(final String user,
      final Credentials credentials,
      final File tokenFile,
      final Logger logger,
      final String group) throws IOException {
    writeCredentialsToFile(credentials, tokenFile, logger);
    try {
      assignPermissions(user, tokenFile, group);
    } catch (final IOException e) {
      // On any error managing token file. delete the file
      tokenFile.delete();
      throw e;
    }
  }

  private void writeCredentialsToFile(final Credentials credentials, final File tokenFile,
      final Logger logger)
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
        } catch (final Throwable t) {
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
   * @param group user group to own the token file
   */
  private void assignPermissions(final String user, final File tokenFile, final String group)
      throws IOException {
    final List<String> changePermissionsCommand = Arrays.asList(
        CHMOD, TOKEN_FILE_PERMISSIONS, tokenFile.getAbsolutePath()
    );
    int result = this.executeAsUser
        .execute(System.getProperty("user.name"), changePermissionsCommand);
    if (result != 0) {
      throw new IOException("Unable to modify permissions. User: " + user);
    }

    final List<String> changeOwnershipCommand = Arrays.asList(
        CHOWN, user + ":" + group, tokenFile.getAbsolutePath()
    );
    result = this.executeAsUser.execute("root", changeOwnershipCommand);
    if (result != 0) {
      throw new IOException("Unable to set ownership. User: " + user);
    }
  }

  private Text getMRTokenRenewerInternal(final JobConf jobConf) throws IOException {
    // Taken from Oozie
    //
    // Getting renewer correctly for JT principal also though JT in hadoop
    // 1.x does not have
    // support for renewing/cancelling tokens
    final String servicePrincipal =
        jobConf.get(RM_PRINCIPAL, jobConf.get(JT_PRINCIPAL));
    final Text renewer;
    if (servicePrincipal != null) {
      String target =
          jobConf.get(HADOOP_YARN_RM, jobConf.get(HADOOP_JOB_TRACKER_2));
      if (target == null) {
        target = jobConf.get(HADOOP_JOB_TRACKER);
      }

      final String addr = NetUtils.createSocketAddr(target).getHostName();
      renewer =
          new Text(SecurityUtil.getServerPrincipal(servicePrincipal, addr));
    } else {
      // No security
      renewer = DEFAULT_RENEWER;
    }

    return renewer;
  }

  private Token<?> getDelegationTokenFromHS(final HSClientProtocol hsProxy)
      throws IOException, InterruptedException {
    final GetDelegationTokenRequest request =
        this.recordFactory.newRecordInstance(GetDelegationTokenRequest.class);
    request.setRenewer(Master.getMasterPrincipal(this.conf));
    final org.apache.hadoop.yarn.api.records.Token mrDelegationToken;
    mrDelegationToken =
        hsProxy.getDelegationToken(request).getDelegationToken();
    return ConverterUtils.convertFromYarn(mrDelegationToken,
        hsProxy.getConnectAddress());
  }

  /**
   * Method to create a metastore client that retries on failures
   */
  private IMetaStoreClient createRetryingMetaStoreClient(final HiveConf hiveConf)
      throws MetaException {
    // Custom hook-loader to return a HiveMetaHook if the table is configured with a custom storage handler
    final HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
      @Override
      public HiveMetaHook getHook(final Table tbl) throws MetaException {
        if (tbl == null) {
          return null;
        }

        try {
          final HiveStorageHandler storageHandler =
              HiveUtils.getStorageHandler(hiveConf, tbl.getParameters().get(META_TABLE_STORAGE));
          return storageHandler == null ? null : storageHandler.getMetaHook();
        } catch (final HiveException e) {
          HadoopSecurityManager_H_2_0.logger.error(e.toString());
          throw new MetaException("Failed to get storage handler: " + e);
        }
      }
    };

    return RetryingMetaStoreClient
        .getProxy(hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
  }
}
