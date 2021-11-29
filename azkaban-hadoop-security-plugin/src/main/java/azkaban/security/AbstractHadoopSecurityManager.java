/*
 * Copyright 2020 LinkedIn Corp.
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
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.log4j.Logger;

/**
 * This class is used as abstract class for all the HadoopSecurityManager versions.
 */
public abstract class AbstractHadoopSecurityManager extends HadoopSecurityManager {

  // The file permissions assigned to a Delegation token file on fetch
  public static final String TOKEN_FILE_PERMISSIONS = "460";
  protected static final String FS_HDFS_IMPL_DISABLE_CACHE =
      "fs.hdfs.impl.disable.cache";
  protected static final String FS_LOCAL_IMPL_DISABLE_CACHE =
      "fs.file.impl.disable.cache";
  // Some hadoop clusters have failover name nodes.
  protected static final String FS_FAILOVER_IMPL_DISABLE_CACHE =
      "fs.failover.impl.disable.cache";
  protected static final String IMPL_DISABLE_CACHE_SUFFIX =
      ".impl.disable.cache";
  protected static final String OBTAIN_JOBHISTORYSERVER_TOKEN =
      "obtain.jobhistoryserver.token";
  protected static final String OTHER_NAMENODES_TO_GET_TOKEN = "other_namenodes";
  protected static final String FQN_SUFFIX_DELIMITER = "/";
  protected UserGroupInformation loginUser;
  protected final ExecuteAsUser executeAsUser;
  protected final Configuration conf;
  protected final ConcurrentMap<String, UserGroupInformation> userUgiMap;
  protected boolean shouldProxy;
  protected boolean securityEnabled;
  public static final String CHOWN = "chown";
  public static final String CHMOD = "chmod";

  // Unable to use slf4j logger as this reference is passed at many places for JobTypeManager
  private static final Logger logger = Logger
      .getLogger(AbstractHadoopSecurityManager.class);

  public AbstractHadoopSecurityManager(final Props props) {
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
    this.userUgiMap = new ConcurrentHashMap<>();
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
          "Unable to proxy as " + userToProxy);
    }
    return user;
  }

  /**
   * This method is used to get property from props object. It will throw an exception when property
   * doesn't exist.
   *
   * @param props
   * @param s
   * @return
   * @throws HadoopSecurityManagerException
   */
  private String verifySecureProperty(final Props props, final String s)
      throws HadoopSecurityManagerException {
    final String value = props.getString(s);
    if (value == null) {
      throw new HadoopSecurityManagerException(s + " not set in properties.");
    }
    return value;
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
      logger.info("Proxy user " + userToProxy
          + " does not exist. Creating new proxy user");
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
   * Get file system as User passed in parameter.
   *
   * @param user
   * @return
   * @throws HadoopSecurityManagerException
   */
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
              return FileSystem.get(AbstractHadoopSecurityManager.this.conf);
            } catch (final IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
      } else {
        fs = FileSystem.get(this.conf);
      }
    } catch (final Exception e) {
      logger.error("Failed to get FileSystem.", e);
      throw new HadoopSecurityManagerException("Failed to get FileSystem. ", e);
    }
    return fs;
  }

  /**
   * This method will verify whether proxy is allowed or not.
   *
   * @return
   */
  public boolean shouldProxy() {
    return this.shouldProxy;
  }

  /**
   * This method is used to get custom credential provider.
   *
   * @param props
   * @param hadoopCred
   * @param jobLogger
   * @param customCredentialProviderName
   * @return
   */
  protected CredentialProvider getCustomCredentialProvider(final Props props,
      final Credentials hadoopCred,
      final Logger jobLogger, final String customCredentialProviderName) {
    String credentialClassName = "unknown class";
    try {
      credentialClassName = props.getString(customCredentialProviderName);
      logger.info("custom credential class name: " + credentialClassName);
      final Class credentialClass = Class.forName(credentialClassName);

      // The credential class must have a constructor accepting 3 parameters, Credentials,
      // Props, and Logger in order.
      final Constructor constructor = credentialClass
          .getConstructor(Credentials.class, Props.class, org.apache.log4j.Logger.class);
      final CredentialProvider customCredential = (CredentialProvider) constructor
          .newInstance(hadoopCred, props, jobLogger);
      return customCredential;
    } catch (final Exception e) {
      logger.error("Encountered error while loading and instantiating "
          + credentialClassName, e);
      throw new IllegalStateException("Encountered error while loading and instantiating "
          + credentialClassName, e);
    }
  }

  /**
   * This method is used to register custom credentials which will be used when doPrefetch method is
   * called.
   *
   * @param props
   * @param hadoopCred
   * @param userToProxy
   * @param jobLogger
   * @param customCredentialProviderName
   */
  protected void registerCustomCredential(final Props props, final Credentials hadoopCred,
      final String userToProxy, final org.apache.log4j.Logger jobLogger,
      final String customCredentialProviderName) {
    final CredentialProvider customCredential = getCustomCredentialProvider(
        props, hadoopCred, jobLogger, customCredentialProviderName);
    final KeyStore keyStore = KeyStoreManager.getInstance().getKeyStore();
    if (keyStore != null) {
      // KeyStore is prepopulated to be used by Credential Provider.
      // This KeyStore is expected especially in case of containerized execution when it is preferred
      // to keep it in-memory of Azkaban user rather than on the file-system of container. This ensures
      // that the user can't access it.
      try {
        ((CredentialProviderWithKeyStore) customCredential).setKeyStore(keyStore);
      } catch (ClassCastException e) {
        logger.error("Encountered error while casting to CredentialProviderWithKeyStore", e);
        throw new IllegalStateException(
            "Encountered error while casting to CredentialProviderWithKeyStore", e);
      } catch (final Exception e) {
        logger.error("Unknown error occurred while setting keyStore", e);
        throw new IllegalStateException("Unknown error occurred while setting keyStore", e);
      }
    }
    customCredential.register(userToProxy);
  }


  /**
   * Fetches the Azkaban KeyStore to be placed in-memory for reuse by all the jobs within a flow in
   * containerized execution. The KeyStore object acquired is placed in KeyStoreManager for future
   * use.
   *
   * @param props Azkaban Props containing CredentialProvider info.
   * @return KeyStore object.
   */
  @Override
  public KeyStore getKeyStore(final Props props) {
    logger.info("Prefetching KeyStore for the flow");
    final Credentials cred = new Credentials();
    final CredentialProviderWithKeyStore customCredential = (CredentialProviderWithKeyStore)
        getCustomCredentialProvider(props, cred, logger,
            Constants.ConfigurationKeys.CUSTOM_CREDENTIAL_NAME);
    final KeyStore keyStore = customCredential.getKeyStore();
    KeyStoreManager.getInstance().setKeyStore(keyStore);
    return keyStore;
  }

  /**
   * This method is used to verify whether Hadoop security is enabled or not.
   *
   * @return
   */
  @Override
  public boolean isHadoopSecurityEnabled() {
    return this.securityEnabled;
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

  /**
   * This method is used to prefetch all required tokens for a job.
   *
   * @param tokenFile
   * @param props
   * @param logger
   * @param userToProxy
   * @throws HadoopSecurityManagerException
   */
  protected void doPrefetch(final File tokenFile, final Props props, final Logger logger,
      final String userToProxy) throws HadoopSecurityManagerException {
    // Create suffix to be added to kerberos principal
    final String suffix = getFQNSuffix(props);

    final String userToProxyFQN = userToProxy + suffix;
    logger.info("Getting hadoop tokens based on props for " + userToProxyFQN);

    final Credentials cred = new Credentials();

    try {
      fetchAllHadoopTokens(userToProxyFQN, userToProxy, props, logger, cred);
      getProxiedUser(userToProxyFQN).doAs((PrivilegedExceptionAction<Void>) () -> {
        registerAllCustomCredentials(userToProxy, props, cred, logger);
        return null;
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

  /**
   * This method is used to get FQN suffix which will be added to proxy user.
   * @param props
   * @return
   */
  protected String getFQNSuffix(Props props) {
    return (null != props.getString(HadoopSecurityManager.DOMAIN_NAME, null)) ?
        FQN_SUFFIX_DELIMITER + kerberosSuffix(props) : "";
  }

  /**
   * Prepare token file. Writes credentials to a token file and sets appropriate permissions to keep
   * the file secure
   *
   * @param user        user to be proxied
   * @param credentials Credentials to be written to file
   * @param tokenFile   file to be written
   * @param logger      logger to use
   * @param group       user group to own the token file
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

  /**
   * This method is used to write all the credentials into file so that the file can be shared with
   * user job process.
   *
   * @param credentials
   * @param tokenFile
   * @param logger
   * @throws IOException
   */
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
   * <p>
   * Step 1. Set file permissions to 460. Readable to self and readable / writable azkaban group
   * Step 2. Set user as owner of file.
   *
   * @param user      user to be proxied
   * @param tokenFile file to be written
   * @param group     user group to own the token file
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

  /*
   * Create a suffix for Kerberos principal, the format is,
   * az_<host name>_<execution id><DOMAIN_NAME>
   */
  protected String kerberosSuffix(final Props props) {
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

  /**
   * This method is used to register all custom credentials for all the versions of Hadoop Security
   * Manager which will extend AbstractHadoopSecurityManager.
   *
   * @param userToProxy
   * @param props
   * @param cred
   * @param logger
   */
  protected void registerAllCustomCredentials(String userToProxy, Props props, Credentials cred,
      Logger logger) {
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

  /**
   * This method is used to cancel token.
   *
   * @param tokenFile
   * @param userToProxy
   * @param logger
   * @throws HadoopSecurityManagerException
   */
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
   * Method to create a metastore client that retries on failures
   */
  protected IMetaStoreClient createRetryingMetaStoreClient(final HiveConf hiveConf)
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
          AbstractHadoopSecurityManager.logger.error(e.toString());
          throw new MetaException("Failed to get storage handler: " + e);
        }
      }
    };

    return RetryingMetaStoreClient
        .getProxy(hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
  }

  /**
   * This method is used to fetch other NameNodes.
   *
   * @param props
   * @return
   */
  protected Optional<String[]> getOtherNameNodes(final Props props) {
    // getting additional name nodes tokens
    final String otherNameNodes = props.get(OTHER_NAMENODES_TO_GET_TOKEN);
    if ((otherNameNodes != null) && (otherNameNodes.length() > 0)) {
      logger.info("Fetching token(s) for other namenode(s): " + otherNameNodes);
      final String[] nameNodeArr = otherNameNodes.split(",");
      return Optional.of(nameNodeArr);
    }
    return Optional.empty();
  }

  /**
   * This method is used to fetch all Hadoop tokens which includes NameNode, JHS, JT and Metastore
   * and add it in cred object.
   *
   * @param userToProxyFQN
   * @param userToProxy
   * @param props
   * @param logger
   * @param cred
   * @throws HadoopSecurityManagerException
   * @throws IOException
   */
  protected abstract void fetchAllHadoopTokens(final String userToProxyFQN,
      final String userToProxy, final Props props, final Logger logger, final Credentials cred)
      throws HadoopSecurityManagerException, IOException, InterruptedException;
}
