package azkaban.flowtrigger.security;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;
import azkaban.utils.Props;
import com.sun.istack.internal.Nullable;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;
import org.apache.log4j.Logger;

/**
 * Prefetches the Tokens by using the security manager that's specified in the configurations.
 *
 * Example Usage:
 *   Props fakeProps = new Props();
 *   AddHardCodedProps.thatEnableTokenPrefetch(fakeProps);
 *   Logger logger = Logger.getLogger(TokenFetcher.class);
 *   TokenFetcher tokenFetcher = new TokenFetcher(fakeProps, fakeProps, logger);
 *   tokenFetcher.createTokenFile(fakeProps, logger);
 */
public class TokenFetcher {
  private HadoopSecurityManager hadoopSecurityManager;
  private boolean shouldPrefetchTokenForProxyUser = false;
  private String userToProxy;

  /**
   * Creates hard-coded props. Values are set so that HadoopSecurityManager_H_2_0 fetches all
   * the tokens for azktest.
   */
  private static void fillValuesThatEnableTokenFetching(final Props props) {
    AddHardCodedProps.thatEnableTokenPrefetch(props);
  }

  /**
   * Loads the HadoopSecurityManager Java class that's specified in the
   * HADOOP_SECURITY_MANAGER_CLASS_PARAM props,
   *
   * @return A HadoopSecurityManager object.
   * @throws RuntimeException : Will throw exception if any errors occur (including not finding the
   * HadoopSecurityManager class in the classpath).
   */
  private static HadoopSecurityManager loadHadoopSecurityManager(final Props props, final Logger log)
      throws RuntimeException {

    final Class<?> hadoopSecurityManagerClass =
        props.getClass(SecurityPropName.HADOOP_SECURITY_MANAGER_CLASS_PARAM, true,
        TokenFetcher.class.getClassLoader());
    log.info("Loading hadoop security manager " + hadoopSecurityManagerClass.getName());
    HadoopSecurityManager hadoopSecurityManager = null;

    try {
      final Method getInstanceMethod = hadoopSecurityManagerClass.getMethod("getInstance", Props.class);
      hadoopSecurityManager = (HadoopSecurityManager) getInstanceMethod.invoke(
          hadoopSecurityManagerClass, props);
    } catch (final InvocationTargetException e) {
      final String errMsg = "Could not instantiate Hadoop Security Manager "
          + hadoopSecurityManagerClass.getName() + e.getCause();
      log.error(errMsg);
      throw new RuntimeException(errMsg, e);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    return hadoopSecurityManager;
  }


  /* Creates a TokenFetcher object, which includes the hadoop security manager specified in the
   * props (for the key HADOOP_SECURITY_MANAGER_CLASS_PARAM).
   *
   * This class is essentially a no-op if either of the following property is false:
   * ENABLE_PROXYING
   * OBTAIN_BINARY_TOKEN
   */
  TokenFetcher(final Props sysProps, final Props jobProps, final Logger logger) {
    final boolean shouldProxy = sysProps.getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    final boolean obtainTokens = sysProps.getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, false);
    userToProxy = jobProps.getString(HadoopSecurityManager.USER_TO_PROXY);

    shouldPrefetchTokenForProxyUser = shouldProxy && obtainTokens;

    if (shouldProxy) {
      try {
        hadoopSecurityManager = loadHadoopSecurityManager(sysProps, logger);
      } catch (final RuntimeException e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to get hadoop security manager!"
            + e.getCause());
      }
    }
  }


  /**
   * Creates a temp file and writes tokens into it. The tokens are fetched as per the properties
   * set in the prop file.
   *
   * @return The newly created temp file that contains the tokens.
   */
  public static File writeTokensToTempFile(
      final HadoopSecurityManager hadoopSecurityManager, final Props props,
      final Logger log) throws HadoopSecurityManagerException {

    File tokenFile = null;
    try {
      tokenFile = File.createTempFile("mr-azkaban", ".token");
    } catch (final Exception e) {
      throw new HadoopSecurityManagerException("Failed to create the token file.", e);
    }

    hadoopSecurityManager.prefetchToken(tokenFile, props, log);

    return tokenFile;
  }

  /**
   * Setup Job Properties when the proxy is enabled
   *
   * @param props all properties
   * @param logger logger handler
   */
  public @Nullable File createTokenFile(final Props props, final Logger logger)
      throws Exception {
    File tokenFile = null;
    if (shouldPrefetchTokenForProxyUser) {
      logger.info("Need to proxy. Getting tokens.");
      // get tokens in to a file, and put the location in props
      tokenFile = writeTokensToTempFile(hadoopSecurityManager, props, logger);
    } else {
      logger.info("props value are set to not enable prefetching");
    }
    return tokenFile;
  }

  // Creates a Tokem File and prints its location
  public static void createTokenFileAndPrintLocation() {
    final Props fakeProps = new Props();
    fillValuesThatEnableTokenFetching(fakeProps);
    final Logger logger = Logger.getLogger(TokenFetcher.class);
    final TokenFetcher tokenFetcher = new TokenFetcher(fakeProps, fakeProps, logger);

    try {
      logger.info("Token file is located at: " +
          tokenFetcher.createTokenFile(fakeProps, logger).getAbsolutePath());
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(final String[] args) {
    TokenFetcher.createTokenFileAndPrintLocation();
  }

}
