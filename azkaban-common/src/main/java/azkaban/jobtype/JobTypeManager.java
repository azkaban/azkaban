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

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobExecutor.NoopJob;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobExecutor.utils.JobExecutionException;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;


public class JobTypeManager {

  private static final Logger LOG = Logger.getLogger(JobTypeManager.class);
  public static final String DEFAULT_JOBTYPEPLUGINDIR = "plugins/jobtypes";
  // need jars.to.include property, will be loaded with user property
  private static final String JOBTYPECONFFILE = "plugin.properties";
  // not exposed to users
  private static final String JOBTYPESYSCONFFILE = "private.properties";
  // common properties for multiple plugins
  private static final String COMMONCONFFILE = "common.properties";
  // common private properties for multiple plugins
  private static final String COMMONSYSCONFFILE = "commonprivate.properties";

  private final String jobTypePluginDir; // the dir for jobtype plugins
  private final ClassLoader parentLoader;
  private final Props globalProperties;
  private JobTypePluginSet pluginSet;

  public JobTypeManager(final String jobtypePluginDir, final Props globalProperties,
      final ClassLoader parentClassLoader) {
    this.jobTypePluginDir = jobtypePluginDir;
    this.parentLoader = parentClassLoader;
    this.globalProperties = globalProperties;

    loadPlugins();
  }

  public void loadPlugins() throws JobTypeManagerException {
    final JobTypePluginSet plugins = new JobTypePluginSet();

    loadDefaultTypes(plugins);
    if (this.jobTypePluginDir != null) {
      final File pluginDir = new File(this.jobTypePluginDir);
      if (pluginDir.exists()) {
        LOG.info("Job type plugin directory set. Loading extra job types from "
                + pluginDir);
        try {
          loadPluginJobTypes(plugins);
        } catch (final Exception e) {
          LOG.info("Plugin jobtypes failed to load. " + e.getCause(), e);
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
    LOG.info("Loading plugin default job types");
    plugins.addPluginClass("command", ProcessJob.class);
    plugins.addPluginClass("javaprocess", JavaProcessJob.class);
    plugins.addPluginClass("noop", NoopJob.class);
  }

  // load Job Types from jobtype plugin dir
  private void loadPluginJobTypes(final JobTypePluginSet plugins)
      throws JobTypeManagerException {
    final File jobPluginsDir = new File(this.jobTypePluginDir);

    if (!jobPluginsDir.exists()) {
      LOG.error("Job type plugin dir " + this.jobTypePluginDir
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
    final File commonJobPropsFile = new File(jobPluginsDir, COMMONCONFFILE);
    if (commonJobPropsFile.exists()) {
      LOG.info("Common plugin job props file " + commonJobPropsFile
          + " found. Attempt to load.");
      try {
        commonPluginJobProps = new Props(this.globalProperties, commonJobPropsFile);
      } catch (final IOException e) {
        throw new JobTypeManagerException(
            "Failed to load common plugin job properties" + e.getCause());
      }
    } else {
      LOG.info("Common plugin job props file " + commonJobPropsFile
          + " not found. Using only globals props");
      commonPluginJobProps = new Props(this.globalProperties);
    }

    // Loads the common properties used by all plugins when loading
    Props commonPluginLoadProps = null;
    final File commonLoadPropsFile = new File(jobPluginsDir, COMMONSYSCONFFILE);
    if (commonLoadPropsFile.exists()) {
      LOG.info("Common plugin load props file " + commonLoadPropsFile
          + " found. Attempt to load.");
      try {
        commonPluginLoadProps = new Props(null, commonLoadPropsFile);
      } catch (final IOException e) {
        throw new JobTypeManagerException(
            "Failed to load common plugin loader properties" + e.getCause());
      }
    } else {
      LOG.info("Common plugin load props file " + commonLoadPropsFile
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
          LOG.error(
              "Failed to load jobtype " + dir.getName() + e.getMessage(), e);
          throw new JobTypeManagerException(e);
        }
      }
    }
  }

  private void loadJobTypes(final File pluginDir, final JobTypePluginSet plugins)
      throws JobTypeManagerException {
    // Directory is the jobtypeName
    final String jobTypeName = pluginDir.getName();
    LOG.info("Loading plugin " + jobTypeName);

    Props pluginJobProps = null;
    Props pluginLoadProps = null;

    final File pluginJobPropsFile = new File(pluginDir, JOBTYPECONFFILE);
    final File pluginLoadPropsFile = new File(pluginDir, JOBTYPESYSCONFFILE);

    if (!pluginLoadPropsFile.exists()) {
      LOG.info("Plugin load props file " + pluginLoadPropsFile
          + " not found.");
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

      pluginLoadProps = new Props(commonPluginLoadProps, pluginLoadPropsFile);
      pluginLoadProps.put("plugin.dir", pluginDir.getAbsolutePath());

      // Adding "plugin.dir" to allow plugin.properties file could read this property. Also, user
      // code could leverage this property as well.
      pluginJobProps.put("plugin.dir", pluginDir.getAbsolutePath());
      pluginLoadProps = PropsUtils.resolveProps(pluginLoadProps);
    } catch (final Exception e) {
      LOG.error("pluginLoadProps to help with debugging: " + pluginLoadProps);
      throw new JobTypeManagerException("Failed to get jobtype properties"
          + e.getMessage(), e);
    }
    // Add properties into the plugin set
    plugins.addPluginLoadProps(jobTypeName, pluginLoadProps);
    if (pluginJobProps != null) {
      plugins.addPluginJobProps(jobTypeName, pluginJobProps);
    }

    final ClassLoader jobTypeLoader =
        loadJobTypeClassLoader(pluginDir, jobTypeName, plugins);
    final String jobtypeClass = pluginLoadProps.get("jobtype.class");

    Class<? extends Job> clazz = null;
    try {
      clazz = (Class<? extends Job>) jobTypeLoader.loadClass(jobtypeClass);
      plugins.addPluginClass(jobTypeName, clazz);
    } catch (final ClassNotFoundException e) {
      throw new JobTypeManagerException(e);
    }

    LOG.info("Verifying job plugin " + jobTypeName);
    try {
      final Props fakeSysProps = new Props(pluginLoadProps);
      final Props fakeJobProps = new Props(pluginJobProps);
      final Job job =
          (Job) Utils.callConstructor(clazz, "dummy", fakeSysProps,
              fakeJobProps, LOG);
    } catch (final Throwable t) {
      LOG.info("Jobtype " + jobTypeName + " failed test!", t);
      throw new JobExecutionException(t);
    }

    LOG.info("Loaded jobtype " + jobTypeName + " " + jobtypeClass);
  }

  /**
   * Creates and loads all plugin resources (jars) into a ClassLoader
   */
  private ClassLoader loadJobTypeClassLoader(final File pluginDir,
      final String jobTypeName, final JobTypePluginSet plugins) {
    // sysconf says what jars/confs to load
    final List<URL> resources = new ArrayList<>();
    final Props pluginLoadProps = plugins.getPluginLoaderProps(jobTypeName);

    try {
      // first global classpath
      LOG.info("Adding global resources for " + jobTypeName);
      final List<String> typeGlobalClassPath =
          pluginLoadProps.getStringList("jobtype.global.classpath", null, ",");
      if (typeGlobalClassPath != null) {
        for (final String jar : typeGlobalClassPath) {
          final URL cpItem = new File(jar).toURI().toURL();
          if (!resources.contains(cpItem)) {
            LOG.info("adding to classpath " + cpItem);
            resources.add(cpItem);
          }
        }
      }

      // type specific classpath
      LOG.info("Adding type resources.");
      final List<String> typeClassPath =
          pluginLoadProps.getStringList("jobtype.classpath", null, ",");
      if (typeClassPath != null) {
        for (final String jar : typeClassPath) {
          final URL cpItem = new File(jar).toURI().toURL();
          if (!resources.contains(cpItem)) {
            LOG.info("adding to classpath " + cpItem);
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
              LOG.info("adding to classpath " + f.toURI().toURL());
            }
          }
        }
      }

      LOG.info("Adding type override resources.");
      for (final File f : pluginDir.listFiles()) {
        if (f.getName().endsWith(".jar")) {
          resources.add(f.toURI().toURL());
          LOG.info("adding to classpath " + f.toURI().toURL());
        }
      }

    } catch (final MalformedURLException e) {
      throw new JobTypeManagerException(e);
    }

    // each job type can have a different class loader
    LOG.info(String
        .format("Classpath for plugin[dir: %s, JobType: %s]: %s", pluginDir, jobTypeName,
            resources));
    final ClassLoader jobTypeLoader =
        new URLClassLoader(resources.toArray(new URL[resources.size()]),
            this.parentLoader);
    return jobTypeLoader;
  }

  public Job buildJobExecutor(final String jobId, Props jobProps, final Logger logger)
      throws JobTypeManagerException {
    // This is final because during build phase, you should never need to swap
    // the pluginSet for safety reasons
    final JobTypePluginSet pluginSet = getJobTypePluginSet();

    Job job = null;
    try {
      final String jobType = jobProps.getString("type");
      if (jobType == null || jobType.length() == 0) {
        /* throw an exception when job name is null or empty */
        throw new JobExecutionException(String.format(
            "The 'type' parameter for job[%s] is null or empty", jobProps));
      }

      logger.info("Building " + jobType + " job executor. ");

      final Class<? extends Object> executorClass = pluginSet.getPluginClass(jobType);
      if (executorClass == null) {
        throw new JobExecutionException(String.format("Job type '" + jobType
                + "' is unrecognized. Could not construct job[%s] of type[%s].",
            jobProps, jobType));
      }

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
      jobProps = PropsUtils.resolveProps(jobProps);

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

      job =
          (Job) Utils.callConstructor(executorClass, jobId, pluginLoadProps,
              jobProps, logger);
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

    return job;
  }

  /**
   * Public for test reasons. Will need to move tests to the same package
   */
  public synchronized JobTypePluginSet getJobTypePluginSet() {
    return this.pluginSet;
  }
}
