/*
 * Copyright 2012 LinkedIn Corp.
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

import azkaban.Constants;
import azkaban.cluster.Cluster;
import azkaban.cluster.ClusterRouter;
import azkaban.cluster.DisabledClusterRouter;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.JavaProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobExecutor.JobClassLoader;
import azkaban.jobExecutor.NoopJob;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobExecutor.utils.JobExecutionException;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

public class JobTypeManager {
  private static final Logger LOGGER = Logger.getLogger(JobTypeManager.class);

  private final String jobTypePluginDir; // the dir for jobtype plugins
  private final ClassLoader parentLoader;
  private final Props globalProperties;
  private final ClusterRouter clusterRouter;
  private JobTypePluginSet pluginSet;

  @VisibleForTesting
  public JobTypeManager(final String jobtypePluginDir, final Props globalProperties,
      final ClassLoader parentClassLoader) {
    this(jobtypePluginDir, globalProperties, parentClassLoader, new DisabledClusterRouter());
  }

  public JobTypeManager(final String jobtypePluginDir, final Props globalProperties,
    final ClassLoader parentClassLoader, ClusterRouter clusterRouter) {
    this.jobTypePluginDir = jobtypePluginDir;
    this.parentLoader = parentClassLoader;
    this.globalProperties = globalProperties;
    this.clusterRouter = clusterRouter;
    loadPlugins();
  }

  public void loadPlugins() throws JobTypeManagerException {
    final JobTypePluginSet plugins = new JobTypePluginSet();

    loadDefaultTypes(plugins);
    if (this.jobTypePluginDir != null) {
      final File pluginDir = new File(this.jobTypePluginDir);
      if (pluginDir.exists()) {
        LOGGER.info("Job type plugin directory set. Loading extra job types from " + pluginDir);
        try {
          loadPluginJobTypes(plugins);
        } catch (final Exception e) {
          LOGGER.info("Plugin jobtypes failed to load. " + e.getCause(), e);
          throw new JobTypeManagerException(e);
        }
      }
    }

    // Swap the plugin set. If exception is thrown, then plugin isn't swapped.
    synchronized (this) {
      this.pluginSet = plugins;
    }
  }

  private void loadDefaultTypes(final JobTypePluginSet plugins)
      throws JobTypeManagerException {
    LOGGER.info("Loading plugin default job types");
    plugins.addPluginClassName("command", ProcessJob.class.getName());
    plugins.addPluginClassName("javaprocess", JavaProcessJob.class.getName());
    plugins.addPluginClassName("noop", NoopJob.class.getName());
  }

  // load Job Types from jobtype plugin dir
  private void loadPluginJobTypes(final JobTypePluginSet plugins)
      throws JobTypeManagerException {
    final File jobPluginsDir = new File(this.jobTypePluginDir);

    if (!jobPluginsDir.exists()) {
      LOGGER.error("Job type plugin dir " + this.jobTypePluginDir
          + " doesn't exist. Will not load any external plugins.");
      return;
    } else if (!jobPluginsDir.isDirectory()) {
      throw new JobTypeManagerException("Job type plugin dir "
          + this.jobTypePluginDir + " is not a directory!");
    } else if (!jobPluginsDir.canRead()) {
      throw new JobTypeManagerException("Job type plugin dir "
          + this.jobTypePluginDir + " is not readable!");
    }

    // Load the common properties used by all jobs that are run
    Props commonPluginJobProps = null;
    final File commonJobPropsFile = new File(jobPluginsDir, Constants.PluginManager.COMMONCONFFILE);
    if (commonJobPropsFile.exists()) {
      LOGGER.info("Common plugin job props file " + commonJobPropsFile
          + " found. Attempt to load.");
      try {
        commonPluginJobProps = new Props(this.globalProperties, commonJobPropsFile);
      } catch (final IOException e) {
        throw new JobTypeManagerException(
            "Failed to load common plugin job properties" + e.getCause());
      }
    } else {
      LOGGER.info("Common plugin job props file " + commonJobPropsFile
          + " not found. Using only globals props");
      commonPluginJobProps = new Props(this.globalProperties);
    }

    // Loads the common properties used by all plugins when loading
    Props commonPluginLoadProps = null;
    final File commonLoadPropsFile = new File(jobPluginsDir, Constants.PluginManager.COMMONSYSCONFFILE);
    if (commonLoadPropsFile.exists()) {
      LOGGER.info("Common plugin load props file " + commonLoadPropsFile
          + " found. Attempt to load.");
      try {
        commonPluginLoadProps = new Props(null, commonLoadPropsFile);
      } catch (final IOException e) {
        throw new JobTypeManagerException(
            "Failed to load common plugin loader properties" + e.getCause());
      }
    } else {
      LOGGER.info("Common plugin load props file " + commonLoadPropsFile
          + " not found. Using empty props.");
      commonPluginLoadProps = new Props();
    }

    plugins.setCommonPluginJobProps(commonPluginJobProps);
    plugins.setCommonPluginLoadProps(commonPluginLoadProps);

    // Loading job types
    for (final File dir : jobPluginsDir.listFiles()) {
      if (dir.isDirectory() && dir.canRead()) {
        try {
          loadJobTypes(dir, plugins);
        } catch (final Exception e) {
          LOGGER.error("Failed to load jobtype " + dir.getName() + e.getMessage(), e);
          throw new JobTypeManagerException(e);
        }
      }
    }
  }

  private void loadJobTypes(final File pluginDir, final JobTypePluginSet plugins)
      throws JobTypeManagerException {
    // Directory is the jobtypeName
    final String jobTypeName = pluginDir.getName();
    LOGGER.info("Loading plugin " + jobTypeName);

    Props pluginJobProps = null;
    Props pluginLoadProps = null;
    Props pluginPrivateProps = null;

    final File pluginJobPropsFile = new File(pluginDir, Constants.PluginManager.CONFFILE);
    final File pluginLoadPropsFile = new File(pluginDir, Constants.PluginManager.SYSCONFFILE);

    if (!pluginLoadPropsFile.exists()) {
      LOGGER.info("Plugin load props file " + pluginLoadPropsFile + " not found.");
      return;
    }

    try {
      final Props commonPluginJobProps = plugins.getCommonPluginJobProps();
      final Props commonPluginLoadProps = plugins.getCommonPluginLoadProps();
      if (pluginJobPropsFile.exists()) {
        pluginJobProps = new Props(commonPluginJobProps, pluginJobPropsFile);
      } else {
        pluginJobProps = new Props(commonPluginJobProps);
      }

      // Set the private props.
      pluginPrivateProps = new Props(null, pluginLoadPropsFile);
      pluginPrivateProps.put("plugin.dir", pluginDir.getAbsolutePath());
      plugins.addPluginPrivateProps(jobTypeName, pluginPrivateProps);

      pluginLoadProps = new Props(commonPluginLoadProps, pluginPrivateProps);

      // Adding "plugin.dir" to allow plugin.properties file could read this property. Also, user
      // code could leverage this property as well.
      pluginJobProps.put("plugin.dir", pluginDir.getAbsolutePath());
      pluginLoadProps = PropsUtils.resolveProps(pluginLoadProps);
    } catch (final Exception e) {
      LOGGER.error("pluginLoadProps to help with debugging: " + pluginLoadProps);
      throw new JobTypeManagerException("Failed to get jobtype properties"
          + e.getMessage(), e);
    }
    // Add properties into the plugin set
    plugins.addPluginLoadProps(jobTypeName, pluginLoadProps);
    if (pluginJobProps != null) {
      plugins.addPluginJobProps(jobTypeName, pluginJobProps);
    }

    final URL[] urls = loadJobTypeClassLoaderURLs(pluginDir, jobTypeName, plugins);
    final ClassLoader jobTypeLoader = new URLClassLoader(urls, parentLoader);

    final String jobtypeClass = pluginLoadProps.get("jobtype.class");
    if (jobtypeClass == null) {
      throw new JobTypeManagerException("Failed to get jobtype property: jobtype.class");
    }

    // load an instance of JobPropsProcessor configured for this jobtype plugin,
    // the JobPropsProcessor instance will be called for each job before it starts to run
    final String jobPropsProcessorClass = pluginLoadProps.get("jobtype.job.props.processor.class");
    if (jobPropsProcessorClass != null && !jobPropsProcessorClass.isEmpty()) {
      Class<? extends JobPropsProcessor> processorClazz;
      try {
        processorClazz = (Class<? extends JobPropsProcessor>) jobTypeLoader.loadClass(jobPropsProcessorClass);
        final JobPropsProcessor jobPropsProcessor = (JobPropsProcessor)
            Utils.callConstructor(processorClazz, pluginLoadProps);
        plugins.addPluginJobPropsProcessor(jobTypeName, jobPropsProcessor);
      } catch (final ClassNotFoundException e) {
        throw new JobTypeManagerException(e);
      }
    }

    plugins.addPluginClassName(jobTypeName, jobtypeClass);
    plugins.addPluginClassLoaderURLs(jobTypeName, urls);
    LOGGER.info("Loaded jobtype " + jobTypeName + " " + jobtypeClass);
  }

  /**
   * Creates and loads all plugin resources (jars) into a ClassLoader
   */
  private URL[] loadJobTypeClassLoaderURLs(final File pluginDir,
      final String jobTypeName, final JobTypePluginSet plugins) {
    // sysconf says what jars/confs to load
    final List<URL> resources = new ArrayList<>();
    final Props pluginLoadProps = plugins.getPluginLoaderProps(jobTypeName);

    try {
      // first global classpath
      LOGGER.info("Adding global resources for " + jobTypeName);
      final List<String> typeGlobalClassPath =
          pluginLoadProps.getStringList("jobtype.global.classpath", null, ",");
      if (typeGlobalClassPath != null) {
        for (final String jar : typeGlobalClassPath) {
          final URL cpItem = new File(jar).toURI().toURL();
          if (!resources.contains(cpItem)) {
            LOGGER.info("adding to classpath " + cpItem);
            resources.add(cpItem);
          }
        }
      }

      // type specific classpath
      LOGGER.info("Adding type resources.");
      final List<String> typeClassPath =
          pluginLoadProps.getStringList("jobtype.classpath", null, ",");
      if (typeClassPath != null) {
        for (final String jar : typeClassPath) {
          final URL cpItem = new File(jar).toURI().toURL();
          if (!resources.contains(cpItem)) {
            LOGGER.info("adding to classpath " + cpItem);
            resources.add(cpItem);
          }
        }
      }
      final List<String> jobtypeLibDirs =
          pluginLoadProps.getStringList("jobtype.lib.dir", null, ",");
      if (jobtypeLibDirs != null) {
        for (final String libDir : jobtypeLibDirs) {
          for (final File f : new File(libDir).listFiles()) {
            if (f.getName().endsWith(".jar")) {
              resources.add(f.toURI().toURL());
              LOGGER.info("adding to classpath " + f.toURI().toURL());
            }
          }
        }
      }

      LOGGER.info("Adding type override resources.");
      for (final File f : pluginDir.listFiles()) {
        if (f.getName().endsWith(".jar")) {
          resources.add(f.toURI().toURL());
          LOGGER.info("adding to classpath " + f.toURI().toURL());
        }
      }

    } catch (final MalformedURLException e) {
      throw new JobTypeManagerException(e);
    }

    // each job type can have a different class loader
    LOGGER.info(String.format("Classpath for plugin[dir: %s, JobType: %s]: %s", pluginDir, jobTypeName,
            resources));
    return resources.toArray(new URL[resources.size()]);
  }

  @VisibleForTesting
  public Job buildJobExecutor(final String jobId, Props jobProps, final Logger logger)
      throws JobTypeManagerException {
    final JobParams jobParams = createJobParams(jobId, jobProps, logger);
    return createJob(jobId, jobParams, logger);
  }

  public JobParams createJobParams(final String jobId, Props jobProps, final Logger logger) {
    // This is final because during build phase, you should never need to swap
    // the pluginSet for safety reasons
    final JobTypePluginSet pluginSet = getJobTypePluginSet();

    try {
      final String jobType = jobProps.getString("type");
      if (jobType == null || jobType.length() == 0) {
        /* throw an exception when job name is null or empty */
        throw new JobExecutionException(String.format(
            "The 'type' parameter for job[%s] is null or empty", jobProps));
      }

      logger.info("Building " + jobType + " job executor. ");

      jobProps = getJobProps(jobProps, pluginSet, jobType);
      final Props pluginLoadProps = getPluginLoadProps(pluginSet, jobType);


      final List<URL> jobClassLoaderUrls = new ArrayList<>();
      // collect jobtype declared dependencies for the job's classloader
      final URL[] jobTypeURLs = pluginSet.getPluginClassLoaderURLs(jobType);
      jobClassLoaderUrls.addAll(Arrays.asList(jobTypeURLs));

      // collect cluster-specific dependencies for the job's classloader
      Cluster targetCluster = null;
      final Collection<String> components = getClusterComponents(jobProps,
          pluginSet.getPluginLoaderProps(jobType), false);
      ClassLoader jobContextClassLoader = this.parentLoader;
      if (!components.isEmpty()) {
        targetCluster = this.clusterRouter.getCluster(jobId, jobProps, logger,
            components);
        if (targetCluster != null && !Cluster.UNKNOWN.equals(targetCluster)) {
          jobContextClassLoader = targetCluster.getSecurityManagerClassLoader();
          jobProps.put(CommonJobProperties.TARGET_CLUSTER_ID, targetCluster.clusterId);
        }
      }
      logger.info(String.format("JobClassLoader URLs: %s", jobClassLoaderUrls.stream()
          .map(URL::toString).collect(Collectors.joining(", "))));
      final ClassLoader jobClassLoader = new JobClassLoader(
          jobClassLoaderUrls.toArray(new URL[jobClassLoaderUrls.size()]),
          jobContextClassLoader, jobId);


      // load the jobtype from JobClassLoader
      final String jobTypeClassName = pluginSet.getPluginClassName(jobType);
      final Class<? extends Object> jobTypeClass = jobClassLoader.loadClass(jobTypeClassName);
      if (jobTypeClass == null) {
        throw new JobExecutionException(String.format("Job type [%s] "
                + "is unrecognized. Could not construct job [%s] of type [%s].",
            jobType, jobId, jobType));
      }

      // inject cluster jars and native libraries into jobs through properties
      jobProps.putAll(
          getClusterSpecificJobProps(targetCluster, jobProps, pluginLoadProps));
      jobProps = PropsUtils.resolveProps(jobProps);

      return new JobParams(jobTypeClass, jobProps, pluginSet.getPluginPrivateProps(jobType),
          pluginLoadProps, jobContextClassLoader);
    } catch (final Exception e) {
      logger.error("Failed to build job executor for job " + jobId
          + e.getMessage());
      throw new JobTypeManagerException("Failed to build job executor for job "
          + jobId, e);
    } catch (final Throwable t) {
      logger.error(
          "Failed to build job executor for job " + jobId + t.getMessage(), t);
      throw new JobTypeManagerException("Failed to build job executor for job "
          + jobId, t);
    }
  }

  private static Props getJobProps(Props jobProps, JobTypePluginSet pluginSet, String jobType) {
    Props pluginJobProps = pluginSet.getPluginJobProps(jobType);
    // For default jobtypes, even though they don't have pluginJobProps configured,
    // they still need to load properties from common.properties file if it's present
    // because common.properties file is global to all jobtypes.
    if (pluginJobProps == null) {
      pluginJobProps = pluginSet.getCommonPluginJobProps();
    }
    if (pluginJobProps != null) {
      for (final String k : pluginJobProps.getKeySet()) {
        if (!jobProps.containsKey(k)) {
          jobProps.put(k, pluginJobProps.get(k));
        }
      }
    }

    final JobPropsProcessor propsProcessor = pluginSet.getPluginJobPropsProcessor(jobType);
    if (propsProcessor != null) {
      jobProps = propsProcessor.process(jobProps);
    }
    return jobProps;
  }

  private static Props getPluginLoadProps(JobTypePluginSet pluginSet, String jobType) {
    Props pluginLoadProps = pluginSet.getPluginLoaderProps(jobType);
    if (pluginLoadProps != null) {
      pluginLoadProps = PropsUtils.resolveProps(pluginLoadProps);
    } else {
      // pluginSet.getCommonPluginLoadProps() will return null if there is no plugins directory.
      // hence assigning default Props() if that's the case
      pluginLoadProps = pluginSet.getCommonPluginLoadProps();
      if (pluginLoadProps == null) {
        pluginLoadProps = new Props();
      }
    }
    return pluginLoadProps;
  }

  /**
   * Create an instance of Job with the given parameters, job id and job logger.
   */
  public static Job createJob(final String jobId, final JobParams jobParams, final Logger logger) {
    Job job;
    try {
      try {
        job =
            (Job) Utils.callConstructor(jobParams.jobClass, jobId, jobParams.pluginLoadProps,
                jobParams.jobProps, jobParams.pluginPrivateProps, logger);
      } catch (final Exception e) {
        logger.info("Failed with 6 inputs with exception e = "
            + e.getMessage());
        job =
            (Job) Utils.callConstructor(jobParams.jobClass, jobId, jobParams.pluginLoadProps,
                jobParams.jobProps, logger);
      }
    } catch (final Exception e) {
      logger.error(String.format("Failed to build job: %s", jobId), e);
      throw new JobTypeManagerException(String.format("Failed to build job %s", jobId), e);
    } catch (final Throwable t) {
      logger.error(String.format("Failed to build job: %s", jobId), t);
      throw new JobTypeManagerException(String.format("Failed to build job %s", jobId), t);
    }
    return job;
  }

  public static final class JobParams {

    public final Class<? extends Object> jobClass;
    public final ClassLoader contextClassLoader;
    public final Props jobProps;
    public final Props pluginLoadProps;
    public final Props pluginPrivateProps;

    public JobParams(final Class<? extends Object> jobClass, final Props jobProps,
                     final Props pluginPrivateProps, final Props pluginLoadProps,
                     final ClassLoader contextClassLoader) {
      this.jobClass = jobClass;
      this.contextClassLoader = contextClassLoader;
      this.jobProps = jobProps;
      this.pluginLoadProps = pluginLoadProps;
      this.pluginPrivateProps = pluginPrivateProps;
    }
  }

  /**
   * Public for test reasons. Will need to move tests to the same package
   */
  public synchronized JobTypePluginSet getJobTypePluginSet() {
    return this.pluginSet;
  }

  /**
   * Expose cluster-specific libraries and native libraries through job properties.
   * if a router is configured, construct the properties based on cluster.properties
   * otherwise, the cluster is implicitly configured, the properties will be based
   * on plugins' private properties.
   */
  private Props getClusterSpecificJobProps(final Cluster cluster, final Props jobProps,
      final Props pluginProps) {
    final Props clusterProps = new Props();

    Props sourceProps;

    if (cluster != null && !Cluster.UNKNOWN.equals(cluster)){
      sourceProps = cluster.getProperties();
      clusterProps.putAll(sourceProps);
    } else {
      // fall back to the existing mechanism if no cluster is found/configured
      sourceProps = pluginProps;
    }

    final Collection<String> components = getClusterComponents(jobProps, pluginProps, true);

    final String javaLibPath = Cluster.getJavaLibraryPath(sourceProps, components);
    if (javaLibPath != null && !javaLibPath.isEmpty()) {
      clusterProps.put(CommonJobProperties.TARGET_CLUSTER_CLASSPATH, javaLibPath);
    }
    final String nativeLibPath = Cluster.getNativeLibraryPath(sourceProps, components);
    if (nativeLibPath != null && !nativeLibPath.isEmpty()) {
      clusterProps.put(CommonJobProperties.TARGET_CLUSTER_NATIVE_LIB, nativeLibPath);
    }
    final String hadoopSecurityManagerClass =
        sourceProps.get(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP);
    if (hadoopSecurityManagerClass != null) {
      clusterProps.put(
          Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, hadoopSecurityManagerClass);
    }

    return clusterProps;
  }

  /**
   * Get the components within a cluster that a job depends on.  Note if
   * jobtype.dependency.components.excluded is set to true, the libraries
   * of the specified components are not exposed to the JVM process that
   * a job may spawn. This is to address jar conflict between
   * HadoopSecurityManager dependencies (hive) and those of individual jobs
   */
  private static Collection<String> getClusterComponents(final Props jobProps,
      final Props pluginProps, final boolean exclusionEnabled) {
    // use ordered set to maintain the classpath order as much as possible
    final Set<String> components = new LinkedHashSet<>();

    if (pluginProps != null) {
      final List<String> jobtypeComponents = pluginProps.getStringList(
          CommonJobProperties.JOBTYPE_CLUSTER_COMPONENTS_DEPENDENCIES, Collections.emptyList(), ",");
      components.addAll(jobtypeComponents);
    }
    if (jobProps != null) {
      final List<String> jobTypeComponents = jobProps.getStringList(
          CommonJobProperties.JOB_CLUSTER_COMPONENTS_DEPENDENCIES, Collections.emptyList(), ",");
      components.addAll(jobTypeComponents);
    }

    if (exclusionEnabled && pluginProps != null) {
      final List<String> excludedComponentsFromJobProcess = pluginProps.getStringList(
          CommonJobProperties.JOBTYPE_CLUSTER_COMPONENTS_DEPENDENCIES_EXCLUDED, Collections.emptyList(), ",");
      components.removeAll(excludedComponentsFromJobProcess);
    }
    return components;
  }
}
