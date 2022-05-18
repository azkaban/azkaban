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

import static azkaban.Constants.JobProperties.EXTRA_HCAT_CLUSTERS;
import static azkaban.Constants.JobProperties.EXTRA_HCAT_LOCATION;
import static azkaban.Constants.HSM_MAX_RETRY_ATTEMPTS;
import static azkaban.Constants.HSM_MAX_RETRY_DELAY_SEC;
import static azkaban.Constants.HSM_RETRY_DELAY_SEC;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.Props;
import azkaban.utils.UndefinedPropertyException;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/**
 * This class is used to fetch tokens using Keytab file and kerberos principal. It fetches
 * delegation token for NameNode, JobHistory Server, JobTracker and HCAT services.
 */
public class HadoopSecurityManager_H_2_0 extends AbstractHadoopSecurityManager {

  // Retry policy builder for fetching Hadoop tokens
  RetryPolicy<Object> retryPolicy = RetryPolicy.builder()
      .handleResult(null)
      .handleResultIf(result -> result == null)
      .withBackoff(HSM_RETRY_DELAY_SEC, HSM_MAX_RETRY_DELAY_SEC, ChronoUnit.SECONDS)
      .withMaxRetries(HSM_MAX_RETRY_ATTEMPTS)
      .onFailedAttempt(e -> logger.error("Token fetch failure {}", e.getLastException()))
      .onRetry(e -> logger.warn("Failure #"+ e.getAttemptCount() +".Retrying."))
      .onFailure(e -> logger.error("Failed to fetch tokens after 5 retries. "
          + e.getException().getMessage()))
      .build();

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

  private static final String AZKABAN_KEYTAB_LOCATION = "proxy.keytab.location";
  private static final String AZKABAN_PRINCIPAL = "proxy.user";
  private final static Logger logger = Logger
      .getLogger(HadoopSecurityManager_H_2_0.class);

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private String keytabLocation;
  private String keytabPrincipal;

  public HadoopSecurityManager_H_2_0(final Props props)
      throws HadoopSecurityManagerException {
    super(props);

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
    logger.info("Hadoop Security Manager initialized");
  }


  /**
   * This method is to fetch hcat token as per the specified hive configuration and then store the
   * token in to the credential store specified .
   *
   * @param userToProxy             String value indicating the name of the user the token will be
   *                                fetched for.
   * @param hiveConf                the configuration based off which the hive client will be
   *                                initialized.
   * @param tokenSignatureOverwrite
   * @param logger                  the logger instance which writes the logging content to the job
   *                                logs.
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
        Failsafe.with(retryPolicy)
            .get(() -> hiveClient.getDelegationToken(userToProxy, UserGroupInformation
            .getLoginUser().getShortUserName()));
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

  /**
   * This method is used to fetch delegation token for JT and add it in cred object.
   *
   * @param userToProxyFQN
   * @param userToProxy
   * @param props
   * @param logger
   * @param cred
   * @throws IOException
   * @throws InterruptedException
   * @throws HadoopSecurityManagerException
   */
  private void fetchJobTrackerToken(final String userToProxyFQN,
      final String userToProxy, final Props props,
      final Logger logger, final Credentials cred)
      throws IOException, InterruptedException, HadoopSecurityManagerException {
    if (props.getBoolean(HadoopSecurityManager.OBTAIN_JOBTRACKER_TOKEN, false)) {
      final JobConf jobConf = new JobConf();
      final JobClient jobClient = new JobClient(jobConf);
      logger.info("Pre-fetching JT token from JobTracker");
      Token<DelegationTokenIdentifier> mrDelegationToken = null;
      try {
        mrDelegationToken = Failsafe.with(retryPolicy)
            .get(()->jobClient.getDelegationToken(getMRTokenRenewerInternal(jobConf)));
      }catch (Exception e){
        logger.error("Failed to get delegation token " + e.getMessage());
      }
      finally {
        jobClient.close();
      }

      if (mrDelegationToken == null) {
        logger.error("Failed to fetch JT token");
        throw new HadoopSecurityManagerException(
            "Failed to fetch JT token for " + userToProxyFQN);
      }

      logger.info(String.format("JT token pre-fetched, token kind: %s, token service: %s",
          mrDelegationToken.getKind(), mrDelegationToken.getService()));
      cred.addToken(mrDelegationToken.getService(), mrDelegationToken);
    }
  }

  @Override
  protected void fetchAllHadoopTokens(final String userToProxyFQN,
      final String userToProxy, final Props props,
      final Logger logger,
      final Credentials cred) throws IOException, InterruptedException,
      HadoopSecurityManagerException {
    logger.info("Fetching all hadoop tokens.");
    fetchMetaStoreToken(userToProxyFQN, userToProxy, props, logger, cred);
    fetchJHSToken(userToProxyFQN, userToProxy, props, logger, cred);
    getProxiedUser(userToProxyFQN).doAs((PrivilegedExceptionAction<Void>) () -> {
      fetchNameNodeToken(userToProxyFQN, userToProxy, props, logger, cred);
      fetchJobTrackerToken(userToProxyFQN, userToProxy, props, logger, cred);
      return null;
    });
  }

  /**
   * This method is used to fetch delegation token for NameNode and add it in cred object.
   *
   * @param userToProxyFQN
   * @param userToProxy
   * @param props
   * @param logger
   * @param cred
   * @throws IOException
   * @throws HadoopSecurityManagerException
   */
  private void fetchNameNodeToken(final String userToProxyFQN,
      final String userToProxy, final Props props,
      final Logger logger,
      final Credentials cred) throws IOException, HadoopSecurityManagerException {
    logger.info("Here is the props for " + HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN +
        ": " + props.getBoolean(HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN));
    if (props.getBoolean(HadoopSecurityManager.OBTAIN_NAMENODE_TOKEN, false)) {
      final String renewer = getMRTokenRenewerInternal(new JobConf()).toString();
      logger.info("Renewer is " + renewer);
      // Get the tokens name node
      fetchNameNodeTokenInternal(renewer, cred, userToProxyFQN, null);
      Optional<String[]> otherNameNodes = getOtherNameNodes(props);
      if (otherNameNodes.isPresent()) {
        String[] nameNodeArr = otherNameNodes.get();
        for (String nameNode : nameNodeArr) {
          fetchNameNodeTokenInternal(renewer, cred, userToProxyFQN,
              new Path(nameNode.trim()).toUri());
          logger.info("Successfully fetched tokens for: " + nameNode);
        }
      }
    } else {
      logger.info(
          HadoopSecurityManager_H_2_0.OTHER_NAMENODES_TO_GET_TOKEN + " was not configured");
    }
  }

  /**
   * fetchNameNodeInternal - With modified UGI which is of the format,
   * <userToProxy>/az_<host name>_<exec_id>
   * Due to this change, the FileSystem cache creates an entry per execution instead of an entry per
   * proxy user. This could blow up the cache very quickly on a busy Executor and cause OOM. To make
   * this worse, the entry in Cache is never used as it is specific to an execution. To avoid this,
   * the FileSystem Cache should be disabled before calling this method.
   *
   * @param renewer
   * @param cred
   * @param userToProxyFQN
   * @param uri
   * @throws IOException
   * @throws HadoopSecurityManagerException
   */
  private void fetchNameNodeTokenInternal(final String renewer, final Credentials cred,
      final String userToProxyFQN, final URI uri)
      throws IOException, HadoopSecurityManagerException {
    FileSystem fs = null;
    try {
      // Use FileSystem.get() instead of newInstance() to ensure cache is not used.
      // .get() method checks if cache is enabled or not, newInstance() does not.
      if (uri == null) {
        fs = Failsafe.with(retryPolicy)
            .get(()->FileSystem.get(conf))
        ;
      } else {
        fs = Failsafe.with(retryPolicy)
            .get(()->FileSystem.get(uri, conf));
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
        // Adding logging of configuration on when exception is encountered.
        logger.info("Hadoop Configuration Values used:\n");
        conf.forEach(s -> {
          logger.info("key:" + s.getKey() + " value:" + s.getValue());
        });
        logger.error("Failed to fetch DFS token for " + userToProxyFQN + "because of  " +e + e.getMessage());
        throw new HadoopSecurityManagerException(
            "Failed to fetch DFS token for " + userToProxyFQN);
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }

  /**
   * This method is used to fetch delegation token for JHS and add it in cred object.
   *
   * @param userToProxyFQN
   * @param userToProxy
   * @param props
   * @param logger
   * @param cred
   * @throws HadoopSecurityManagerException
   * @throws IOException
   */
  private void fetchJHSToken(final String userToProxyFQN,
      final String userToProxy, final Props props, final Logger logger, final Credentials cred)
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
        logger.error("Failed to fetch JH token" + e.getMessage());
        throw new HadoopSecurityManagerException(
            "Failed to fetch JH token for " + userToProxyFQN);
      }

      if (hsProxy instanceof Closeable) {
        // HSClientProtocol is not closable, but its only implementation, HSClientProtocolPBClientImpl, is
        ((Closeable) hsProxy).close();
      }

      if (jhsdt == null) {
        logger.error("getDelegationTokenFromHS() returned null");
        throw new HadoopSecurityManagerException(
            "Unable to fetch JH token for " + userToProxyFQN);
      }

      logger.info(String
          .format("JH token from job history server pre-fetched, token Kind: %s, token service: %s",
              jhsdt.getKind(), jhsdt.getService()));

      cred.addToken(jhsdt.getService(), jhsdt);
    }
  }

  /**
   * This method is used to fetch delegation token for MetaStore and add it in cred object.
   *
   * @param userToProxyFQN
   * @param userToProxy
   * @param props
   * @param logger
   * @param cred
   * @throws HadoopSecurityManagerException
   */
  private void fetchMetaStoreToken(final String userToProxyFQN,
      final String userToProxy, final Props props, final Logger logger,
      final Credentials cred)
      throws HadoopSecurityManagerException {
    if (props.getBoolean(HadoopSecurityManager.OBTAIN_HCAT_TOKEN, false)) {
      try {

        // first we fetch and save the default hcat token.
        logger.info("Pre-fetching default hive metastore token from hive");

        HiveConf hiveConf = new HiveConf();
        Token<DelegationTokenIdentifier> hcatToken =
            fetchHcatToken(userToProxyFQN, hiveConf, null, logger);

        cred.addToken(hcatToken.getService(), hcatToken);

        // Added support for extra_hcat_clusters
        final List<String> extraHcatClusters = props.getStringListFromCluster(EXTRA_HCAT_CLUSTERS);
        int extraHcatTokenCount = 0;
        if (Collections.EMPTY_LIST != extraHcatClusters) {
          logger.info("Need to pre-fetch extra metastore tokens from extra hive clusters.");

          // start to process the user inputs.
          for (final String thriftUrls : extraHcatClusters) {
            logger.info("Pre-fetching metastore token from cluster : " + thriftUrls);

            hiveConf = new HiveConf();
            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUrls);
            logger.info(hiveConf.getAllProperties() + " " + userToProxyFQN );
            try {
              hcatToken = fetchHcatToken(userToProxyFQN, hiveConf, thriftUrls, logger);
              cred.addToken(hcatToken.getService(), hcatToken);
              ++extraHcatTokenCount;
            } catch (Exception e) {
              logger.error("Failed to fetch extra metastore tokens from : " + thriftUrls
                  + e.getMessage());
            }
          }
          if (0 == extraHcatTokenCount) {
            throw new HadoopSecurityManagerException("No extra metastore token could be fetched.");
          }
        } else {
          // Only if EXTRA_HCAT_CLUSTERS
          logger.info("Extra hcat clusters provided: " + extraHcatClusters);
          final List<String> extraHcatLocations =
              props.getStringList(EXTRA_HCAT_LOCATION);
          if (Collections.EMPTY_LIST != extraHcatLocations) {
            logger.info("Need to pre-fetch extra metastore tokens from hive.");

            // start to process the user inputs.
            for (final String thriftUrl : extraHcatLocations) {
              logger.info("Pre-fetching metastore token from : " + thriftUrl);

              hiveConf = new HiveConf();
              hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, thriftUrl);
              try {
                hcatToken =
                    fetchHcatToken(userToProxyFQN, hiveConf, thriftUrl, logger);
                cred.addToken(hcatToken.getService(), hcatToken);
                ++extraHcatTokenCount;
              } catch (Exception e) {
                logger.error("Failed to fetch extra metastore tokens from : " + thriftUrl, e);
              }
            }
            if (0 == extraHcatTokenCount) {
              throw new HadoopSecurityManagerException("No extra metastore token could be fetched.");
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
    logger.info(hsProxy.toString());
    final GetDelegationTokenRequest request =
        this.recordFactory.newRecordInstance(GetDelegationTokenRequest.class);
    request.setRenewer(Master.getMasterPrincipal(this.conf));
    logger.info(request.getRenewer() + Master.getMasterPrincipal(this.conf));
    final org.apache.hadoop.yarn.api.records.Token mrDelegationToken;
    mrDelegationToken =
        Failsafe.with(retryPolicy)
            .get(()->hsProxy.getDelegationToken(request).getDelegationToken());
    return ConverterUtils.convertFromYarn(mrDelegationToken,
        hsProxy.getConnectAddress());
  }
}
