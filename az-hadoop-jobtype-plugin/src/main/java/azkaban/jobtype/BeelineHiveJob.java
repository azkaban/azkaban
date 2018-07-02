package azkaban.jobtype;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

public class BeelineHiveJob extends JavaProcessJob {

  public static final String HIVE_SCRIPT = "hive.script";
  public static final String HIVE_URL = "hive.url";
  public static final String HADOOP_SECURE_HIVE_WRAPPER =
      "azkaban.jobtype.HadoopSecureBeelineWrapper";
  private static final String HIVECONF_PARAM_PREFIX = "hiveconf.";
  private static final String HIVEVAR_PARAM_PREFIX = "hivevar.";
  private String userToProxy = null;
  private boolean shouldProxy = false;
  private boolean obtainTokens = false;
  private File tokenFile = null;

  private HadoopSecurityManager hadoopSecurityManager;

  private boolean debug = false;

  public BeelineHiveJob(final String jobid, final Props sysProps, final Props jobProps,
      final Logger log)
      throws IOException {

    super(jobid, sysProps, jobProps, log);

    getJobProps().put(CommonJobProperties.JOB_ID, jobid);

    this.shouldProxy = getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
    getJobProps().put(HadoopSecurityManager.ENABLE_PROXYING, Boolean.toString(this.shouldProxy));
    this.obtainTokens = getSysProps().getBoolean(HadoopSecurityManager.OBTAIN_BINARY_TOKEN, false);

    this.debug = getJobProps().getBoolean("debug", false);

    if (this.shouldProxy) {
      getLog().info("Initiating hadoop security manager.");
      try {
        this.hadoopSecurityManager = HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), log);
      } catch (final RuntimeException e) {
        throw new RuntimeException("Failed to get hadoop security manager!" + e);
      }
    }
  }

  private static String getSourcePathFromClass(final Class<?> containedClass) {
    File file =
        new File(containedClass.getProtectionDomain().getCodeSource()
            .getLocation().getPath());

    if (!file.isDirectory() && file.getName().endsWith(".class")) {
      final String name = containedClass.getName();
      final StringTokenizer tokenizer = new StringTokenizer(name, ".");
      while (tokenizer.hasMoreTokens()) {
        tokenizer.nextElement();
        file = file.getParentFile();
      }

      return file.getPath();
    } else {
      return containedClass.getProtectionDomain().getCodeSource().getLocation()
          .getPath();
    }
  }

  @Override
  public void run() throws Exception {
    HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(),
        getWorkingDirectory());

    if (this.shouldProxy && this.obtainTokens) {
      this.userToProxy = getJobProps().getString("user.to.proxy");
      getLog().info("Need to proxy. Getting tokens.");
      // get tokens in to a file, and put the location in props
      final Props props = new Props();
      props.putAll(getJobProps());
      props.putAll(getSysProps());
      HadoopJobUtils.addAdditionalNamenodesToPropsFromMRJob(props, getLog());
      this.tokenFile = HadoopJobUtils.getHadoopTokens(this.hadoopSecurityManager, props, getLog());
      getJobProps().put("env." + HADOOP_TOKEN_FILE_LOCATION,
          this.tokenFile.getAbsolutePath());
    }

    try {
      super.run();
    } catch (final Throwable t) {
      t.printStackTrace();
      getLog().error("caught error running the job");
      throw new Exception(t);
    } finally {
      if (this.tokenFile != null) {
        HadoopJobUtils
            .cancelHadoopTokens(this.hadoopSecurityManager, this.userToProxy, this.tokenFile,
                getLog());
        if (this.tokenFile.exists()) {
          this.tokenFile.delete();
        }
      }
    }
  }

  @Override
  protected String getJavaClass() {
    return HADOOP_SECURE_HIVE_WRAPPER;
  }

  @Override
  protected String getJVMArguments() {
    String args = super.getJVMArguments();

    final String typeUserGlobalJVMArgs =
        getJobProps().getString("jobtype.global.jvm.args", null);
    if (typeUserGlobalJVMArgs != null) {
      args += " " + typeUserGlobalJVMArgs;
    }
    final String typeSysGlobalJVMArgs =
        getSysProps().getString("jobtype.global.jvm.args", null);
    if (typeSysGlobalJVMArgs != null) {
      args += " " + typeSysGlobalJVMArgs;
    }
    final String typeUserJVMArgs = getJobProps().getString("jobtype.jvm.args", null);
    if (typeUserJVMArgs != null) {
      args += " " + typeUserJVMArgs;
    }
    final String typeSysJVMArgs = getSysProps().getString("jobtype.jvm.args", null);
    if (typeSysJVMArgs != null) {
      args += " " + typeSysJVMArgs;
    }

    if (this.shouldProxy) {
      info("Setting up secure proxy info for child process");
      String secure;
      secure =
          " -D" + HadoopSecurityManager.USER_TO_PROXY + "="
              + getJobProps().getString(HadoopSecurityManager.USER_TO_PROXY);
      final String extraToken =
          getSysProps().getString(HadoopSecurityManager.OBTAIN_BINARY_TOKEN,
              "false");
      if (extraToken != null) {
        secure +=
            " -D" + HadoopSecurityManager.OBTAIN_BINARY_TOKEN + "="
                + extraToken;
      }

      secure += " -Dmapreduce.job.credentials.binary=" + this.tokenFile;
      info("Secure settings = " + secure);
      args += secure;
    } else {
      info("Not setting up secure proxy info for child process");
    }

    return args;
  }

  @Override
  protected String getMainArguments() {
    final ArrayList<String> list = new ArrayList<>();

    list.add("-u");
    list.add(getJobProps().getString(HIVE_URL));

    list.add("-n");
    if (this.shouldProxy) {
      list.add(this.userToProxy);
    } else {
      list.add(System.getProperty("user.name"));
    }

    list.add("-p");
    list.add("DUMMY");

    // for hiveconf
    final Map<String, String> map = getHiveConf();
    if (map != null) {
      for (final Map.Entry<String, String> entry : map.entrySet()) {
        list.add("--hiveconf");
        list.add(StringUtils.shellQuote(
            entry.getKey() + "=" + entry.getValue(), StringUtils.SINGLE_QUOTE));
      }
    }

    if (this.debug) {
      list.add("--hiveconf");
      list.add("hive.root.logger=INFO,console");
    }

    // for hivevar
    final Map<String, String> hiveVarMap = getHiveVar();
    if (hiveVarMap != null) {
      for (final Map.Entry<String, String> entry : hiveVarMap.entrySet()) {
        list.add("--hivevar");
        list.add(StringUtils.shellQuote(
            entry.getKey() + "=" + entry.getValue(), StringUtils.SINGLE_QUOTE));
      }
    }

    list.add("-d");
    list.add("org.apache.hive.jdbc.HiveDriver");

    list.add("-f");
    list.add(getScript());

    if (this.shouldProxy) {
      list.add("-a");
      list.add("delegationToken");
    }

    return StringUtils.join((Collection<String>) list, " ");
  }

  @Override
  protected List<String> getClassPaths() {

    final List<String> classPath = super.getClassPaths();

    // To add az-core jar classpath
    classPath.add(getSourcePathFromClass(Props.class));

    // To add az-common jar classpath
    classPath.add(getSourcePathFromClass(JavaProcessJob.class));
    classPath.add(getSourcePathFromClass(HadoopSecureWrapperUtils.class));
    classPath.add(getSourcePathFromClass(HadoopSecureBeelineWrapper.class));
    classPath.add(getSourcePathFromClass(HadoopSecurityManager.class));

    final String loggerPath = getSourcePathFromClass(org.apache.log4j.Logger.class);
    if (!classPath.contains(loggerPath)) {
      classPath.add(loggerPath);
    }

    classPath.add(HadoopConfigurationInjector.getPath(getJobProps(),
        getWorkingDirectory()));
    final List<String> typeClassPath =
        getSysProps().getStringList("jobtype.classpath", null, ",");
    if (typeClassPath != null) {
      // fill in this when load this jobtype
      final String pluginDir = getSysProps().get("plugin.dir");
      for (final String jar : typeClassPath) {
        File jarFile = new File(jar);
        if (!jarFile.isAbsolute()) {
          jarFile = new File(pluginDir + File.separatorChar + jar);
        }

        if (!classPath.contains(jarFile.getAbsoluteFile().toString())) {
          classPath.add(jarFile.getAbsolutePath());
        }
      }
    }

    final List<String> typeGlobalClassPath =
        getSysProps().getStringList("jobtype.global.classpath", null, ",");
    if (typeGlobalClassPath != null) {
      for (final String jar : typeGlobalClassPath) {
        if (!classPath.contains(jar)) {
          classPath.add(jar);
        }
      }
    }

    return classPath;
  }

  protected String getScript() {
    return getJobProps().getString(HIVE_SCRIPT);
  }

  protected Map<String, String> getHiveConf() {
    return getJobProps().getMapByPrefix(HIVECONF_PARAM_PREFIX);
  }

  protected Map<String, String> getHiveVar() {
    return getJobProps().getMapByPrefix(HIVEVAR_PARAM_PREFIX);
  }

  /**
   * This cancel method, in addition to the default canceling behavior, also kills the MR jobs
   * launched by Hive
   * on Hadoop
   */
  @Override
  public void cancel() throws InterruptedException {
    super.cancel();

    info("Cancel called.  Killing the Hive launched MR jobs on the cluster");

    final String azExecId = this.jobProps.getString(CommonJobProperties.EXEC_ID);
    final String logFilePath =
        String.format("%s/_job.%s.%s.log", getWorkingDirectory(), azExecId,
            getId());
    info("log file path is: " + logFilePath);

    HadoopJobUtils
        .proxyUserKillAllSpawnedHadoopJobs(logFilePath, this.jobProps, this.tokenFile, getLog());
  }
}

