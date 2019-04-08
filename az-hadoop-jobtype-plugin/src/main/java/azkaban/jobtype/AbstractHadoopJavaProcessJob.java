package azkaban.jobtype;

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import java.io.File;
import java.util.List;
import org.apache.log4j.Logger;

import static org.apache.hadoop.security.UserGroupInformation.*;


/**
 * Abstract Hadoop Job for Job PlugIn
 */
public abstract class AbstractHadoopJavaProcessJob extends JavaProcessJob {

  protected HadoopSecurityManager hadoopSecurityManager;

  //TODO: Refactoring the code and move the following attributes into the HadoopSecurityManager
  protected boolean shouldProxy = false;
  protected boolean obtainTokens = false;
  protected File tokenFile = null;
  protected String userToProxy = null;

  public AbstractHadoopJavaProcessJob(String jobid, Props sysProps, Props jobProps, Logger logger) {
    super(jobid, sysProps, jobProps, logger);
    shouldProxy = getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    getJobProps().put(HadoopSecurityManager.ENABLE_PROXYING, Boolean.toString(shouldProxy));
    obtainTokens = getSysProps().getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, false);
    if (shouldProxy) {
      getLog().info("Initiating hadoop security manager.");
      try {
        hadoopSecurityManager = HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), logger);
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to get hadoop security manager!"
            + e.getCause());
      }
    }
  }


  @Override
  public void run() throws Exception {
    setupPropsForProxy();

    try {
      super.run();
    } catch (Throwable t) {
      t.printStackTrace();
      getLog().error("caught error running the job", t);
      throw new Exception(t);
    } finally {
      HadoopJobUtils.cancelHadoopTokens(hadoopSecurityManager, userToProxy, tokenFile, getLog());
    }
  }

  protected String getJVMProxySecureArgument() {
    String secure = "";

    if (shouldProxy) {
      info("Setting up secure proxy info for child process");
      secure = " -D" + HadoopSecurityManager.USER_TO_PROXY
          + "=" + getJobProps().getString(HadoopSecurityManager.USER_TO_PROXY);

      String extraToken = getSysProps().getString(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, "false");
      if (extraToken != null) {
        secure += " -D" + HadoopSecurityManager.OBTAIN_BINARY_TOKEN + "=" + extraToken;
      }
      info("Secure settings = " + secure);
    } else {
      info("Not setting up secure proxy info for child process");
    }

    return secure;
  }

  protected void setupPropsForProxy() throws Exception {
    if (shouldProxy && obtainTokens) {
      userToProxy = getJobProps().getString(HadoopSecurityManager.USER_TO_PROXY);
      getLog().info("Need to proxy. Getting tokens.");
      // get tokens in to a file, and put the location in props
      Props props = new Props();
      props.putAll(getJobProps());
      props.putAll(getSysProps());

      addAdditionalNamenodesToProps(props);

      tokenFile = HadoopJobUtils.getHadoopTokens(hadoopSecurityManager, props, getLog());
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION, tokenFile.getAbsolutePath());
    }
  }

  protected void addAdditionalNamenodesToProps(final Props props) {
    HadoopJobUtils.addAdditionalNamenodesToPropsFromMRJob(props, getLog());
  }

  /**
   * Merge typeClassPath List into the classPath List
   * @param classPath destination list
   * @param typeClassPath source list
   */
  protected void mergeTypeClassPaths(List<String>  classPath, final List<String> typeClassPath) {
    if (typeClassPath != null) {
      // fill in this when load this jobtype
      final String pluginDir = getSysProps().get("plugin.dir");
      for (String jar : typeClassPath) {
        File jarFile = new File(jar);
        if (!jarFile.isAbsolute()) {
          jarFile = new File(pluginDir + File.separatorChar + jar);
        }

        if (!classPath.contains(jarFile.getAbsolutePath())) {
          classPath.add(jarFile.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Merge typeGlobalClassPath List into the classPath List
   * @param classPath destination list
   * @param typeGlobalClassPath source list
   */
  protected void mergeTypeGlobalClassPaths(List<String> classPath, final List<String> typeGlobalClassPath) {
    if (typeGlobalClassPath != null) {
      for (String jar : typeGlobalClassPath) {
        if (!classPath.contains(jar)) {
          classPath.add(jar);
        }
      }
    }
  }
}
