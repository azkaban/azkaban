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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.jobExecutor.Job;
import azkaban.jobExecutor.NoopJob;
import azkaban.jobExecutor.ProcessJob;
import azkaban.jobExecutor.PythonJob;
import azkaban.jobExecutor.RubyJob;
import azkaban.jobExecutor.ScriptJob;
import azkaban.jobExecutor.utils.JobExecutionException;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.Utils;

public class JobTypeManager {
  private final String jobTypePluginDir; // the dir for jobtype plugins
  private final ClassLoader parentLoader;

  public static final String DEFAULT_JOBTYPEPLUGINDIR = "plugins/jobtypes";
  // need jars.to.include property, will be loaded with user property
  private static final String JOBTYPECONFFILE = "plugin.properties";
  // not exposed to users
  private static final String JOBTYPESYSCONFFILE = "private.properties";
  // common properties for multiple plugins
  private static final String COMMONCONFFILE = "common.properties";
  // common private properties for multiple plugins
  private static final String COMMONSYSCONFFILE = "commonprivate.properties";
  private static final Logger logger = Logger.getLogger(JobTypeManager.class);

  private JobTypePluginSet pluginSet;
  private Props globalProperties;

  public JobTypeManager(String jobtypePluginDir, Props globalProperties,
      ClassLoader parentClassLoader) {
    this.jobTypePluginDir = jobtypePluginDir;
    this.parentLoader = parentClassLoader;
    this.globalProperties = globalProperties;

    loadPlugins();
  }

  public void loadPlugins() throws JobTypeManagerException {
    JobTypePluginSet plugins = new JobTypePluginSet();

    loadDefaultTypes(plugins);
    if (jobTypePluginDir != null) {
      File pluginDir = new File(jobTypePluginDir);
      if (pluginDir.exists()) {
        logger
            .info("Job type plugin directory set. Loading extra job types from "
                + pluginDir);
        try {
          loadPluginJobTypes(plugins);
        } catch (Exception e) {
          logger.info("Plugin jobtypes failed to load. " + e.getCause(), e);
          throw new JobTypeManagerException(e);
        }
      }
    }

    // Swap the plugin set. If exception is thrown, then plugin isn't swapped.
    synchronized (this) {
      pluginSet = plugins;
    }
  }

  private void loadDefaultTypes(JobTypePluginSet plugins)
      throws JobTypeManagerException {
    logger.info("Loading plugin default job types");
    plugins.addPluginClass("command", ProcessJob.class);
    plugins.addPluginClass("javaprocess", JavaProcessJob.class);
    plugins.addPluginClass("noop", NoopJob.class);
    plugins.addPluginClass("python", PythonJob.class);
    plugins.addPluginClass("ruby", RubyJob.class);
    plugins.addPluginClass("script", ScriptJob.class);
  }

  // load Job Types from jobtype plugin dir
  private void loadPluginJobTypes(JobTypePluginSet plugins)
      throws JobTypeManagerException {
    File jobPluginsDir = new File(jobTypePluginDir);

    if (!jobPluginsDir.exists()) {
      logger.error("Job type plugin dir " + jobTypePluginDir
          + " doesn't exist. Will not load any external plugins.");
      return;
    } else if (!jobPluginsDir.isDirectory()) {
      throw new JobTypeManagerException("Job type plugin dir "
          + jobTypePluginDir + " is not a directory!");
    } else if (!jobPluginsDir.canRead()) {
      throw new JobTypeManagerException("Job type plugin dir "
          + jobTypePluginDir + " is not readable!");
    }

    // Load the common properties used by all jobs that are run
    Props commonPluginJobProps = null;
    File commonJobPropsFile = new File(jobPluginsDir, COMMONCONFFILE);
    if (commonJobPropsFile.exists()) {
      logger.info("Common plugin job props file " + commonJobPropsFile
          + " found. Attempt to load.");
      try {
        commonPluginJobProps = new Props(globalProperties, commonJobPropsFile);
      } catch (IOException e) {
        throw new JobTypeManagerException(
            "Failed to load common plugin job properties" + e.getCause());
      }
    } else {
      logger.info("Common plugin job props file " + commonJobPropsFile
          + " not found. Using empty props.");
      commonPluginJobProps = new Props();
    }

    // Loads the common properties used by all plugins when loading
    Props commonPluginLoadProps = null;
    File commonLoadPropsFile = new File(jobPluginsDir, COMMONSYSCONFFILE);
    if (commonLoadPropsFile.exists()) {
      logger.info("Common plugin load props file " + commonLoadPropsFile
          + " found. Attempt to load.");
      try {
        commonPluginLoadProps = new Props(null, commonLoadPropsFile);
      } catch (IOException e) {
        throw new JobTypeManagerException(
            "Failed to load common plugin loader properties" + e.getCause());
      }
    } else {
      logger.info("Common plugin load props file " + commonLoadPropsFile
          + " not found. Using empty props.");
      commonPluginLoadProps = new Props();
    }

    plugins.setCommonPluginJobProps(commonPluginJobProps);
    plugins.setCommonPluginLoadProps(commonPluginLoadProps);

    // Loading job types
    for (File dir : jobPluginsDir.listFiles()) {
      if (dir.isDirectory() && dir.canRead()) {
        try {
          loadJobTypes(dir, plugins);
        } catch (Exception e) {
          logger.error(
              "Failed to load jobtype " + dir.getName() + e.getMessage(), e);
          throw new JobTypeManagerException(e);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void loadJobTypes(File pluginDir, JobTypePluginSet plugins)
      throws JobTypeManagerException {
    // Directory is the jobtypeName
    String jobTypeName = pluginDir.getName();
    logger.info("Loading plugin " + jobTypeName);

    Props pluginJobProps = null;
    Props pluginLoadProps = null;

    File pluginJobPropsFile = new File(pluginDir, JOBTYPECONFFILE);
    File pluginLoadPropsFile = new File(pluginDir, JOBTYPESYSCONFFILE);

    if (!pluginLoadPropsFile.exists()) {
      logger.info("Plugin load props file " + pluginLoadPropsFile
          + " not found.");
      return;
    }

    try {
      Props commonPluginJobProps = plugins.getCommonPluginJobProps();
      Props commonPluginLoadProps = plugins.getCommonPluginLoadProps();
      if (pluginJobPropsFile.exists()) {
        pluginJobProps = new Props(commonPluginJobProps, pluginJobPropsFile);
      } else {
        pluginJobProps = new Props(commonPluginJobProps);
      }

      pluginLoadProps = new Props(commonPluginLoadProps, pluginLoadPropsFile);
      pluginLoadProps.put("plugin.dir", pluginDir.getAbsolutePath());
      pluginLoadProps = PropsUtils.resolveProps(pluginLoadProps);
    } catch (Exception e) {
      logger.error("pluginLoadProps to help with debugging: " + pluginLoadProps);
      throw new JobTypeManagerException("Failed to get jobtype properties"
          + e.getMessage(), e);
    }
    // Add properties into the plugin set
    plugins.addPluginLoadProps(jobTypeName, pluginLoadProps);
    if (pluginJobProps != null) {
      plugins.addPluginJobProps(jobTypeName, pluginJobProps);
    }

    ClassLoader jobTypeLoader =
        loadJobTypeClassLoader(pluginDir, jobTypeName, plugins);
    String jobtypeClass = pluginLoadProps.get("jobtype.class");

    Class<? extends Job> clazz = null;
    try {
      clazz = (Class<? extends Job>) jobTypeLoader.loadClass(jobtypeClass);
      plugins.addPluginClass(jobTypeName, clazz);
    } catch (ClassNotFoundException e) {
      throw new JobTypeManagerException(e);
    }

    logger.info("Verifying job plugin " + jobTypeName);
    try {
      Props fakeSysProps = new Props(pluginLoadProps);
      Props fakeJobProps = new Props(pluginJobProps);
      @SuppressWarnings("unused")
      Job job =
          (Job) Utils.callConstructor(clazz, "dummy", fakeSysProps,
              fakeJobProps, logger);
    } catch (Throwable t) {
      logger.info("Jobtype " + jobTypeName + " failed test!", t);
      throw new JobExecutionException(t);
    }

    logger.info("Loaded jobtype " + jobTypeName + " " + jobtypeClass);
  }

  /**
   * Creates and loads all plugin resources (jars) into a ClassLoader
   *
   * @param pluginDir
   * @param jobTypeName
   * @param plugins
   * @return
   */
  private ClassLoader loadJobTypeClassLoader(File pluginDir,
      String jobTypeName, JobTypePluginSet plugins) {
    // sysconf says what jars/confs to load
    List<URL> resources = new ArrayList<URL>();
    Props pluginLoadProps = plugins.getPluginLoaderProps(jobTypeName);

    try {
      // first global classpath
      logger.info("Adding global resources for " + jobTypeName);
      List<String> typeGlobalClassPath =
          pluginLoadProps.getStringList("jobtype.global.classpath", null, ",");
      if (typeGlobalClassPath != null) {
        for (String jar : typeGlobalClassPath) {
          URL cpItem = new File(jar).toURI().toURL();
          if (!resources.contains(cpItem)) {
            logger.info("adding to classpath " + cpItem);
            resources.add(cpItem);
          }
        }
      }

      // type specific classpath
      logger.info("Adding type resources.");
      List<String> typeClassPath =
          pluginLoadProps.getStringList("jobtype.classpath", null, ",");
      if (typeClassPath != null) {
        for (String jar : typeClassPath) {
          URL cpItem = new File(jar).toURI().toURL();
          if (!resources.contains(cpItem)) {
            logger.info("adding to classpath " + cpItem);
            resources.add(cpItem);
          }
        }
      }
      List<String> jobtypeLibDirs =
          pluginLoadProps.getStringList("jobtype.lib.dir", null, ",");
      if (jobtypeLibDirs != null) {
        for (String libDir : jobtypeLibDirs) {
          for (File f : new File(libDir).listFiles()) {
            if (f.getName().endsWith(".jar")) {
              resources.add(f.toURI().toURL());
              logger.info("adding to classpath " + f.toURI().toURL());
            }
          }
        }
      }

      logger.info("Adding type override resources.");
      for (File f : pluginDir.listFiles()) {
        if (f.getName().endsWith(".jar")) {
          resources.add(f.toURI().toURL());
          logger.info("adding to classpath " + f.toURI().toURL());
        }
      }

    } catch (MalformedURLException e) {
      throw new JobTypeManagerException(e);
    }

    // each job type can have a different class loader
    ClassLoader jobTypeLoader =
        new URLClassLoader(resources.toArray(new URL[resources.size()]),
            parentLoader);
    return jobTypeLoader;
  }

  public Job buildJobExecutor(String jobId, Props jobProps, Logger logger)
      throws JobTypeManagerException {
    // This is final because during build phase, you should never need to swap
    // the pluginSet for safety reasons
    final JobTypePluginSet pluginSet = getJobTypePluginSet();

    Job job = null;
    try {
      String jobType = jobProps.getString("type");
      if (jobType == null || jobType.length() == 0) {
        /* throw an exception when job name is null or empty */
        throw new JobExecutionException(String.format(
            "The 'type' parameter for job[%s] is null or empty", jobProps,
            logger));
      }

      logger.info("Building " + jobType + " job executor. ");

      Class<? extends Object> executorClass = pluginSet.getPluginClass(jobType);
      if (executorClass == null) {
        throw new JobExecutionException(String.format("Job type '" + jobType
            + "' is unrecognized. Could not construct job[%s] of type[%s].",
            jobProps, jobType));
      }

      // TODO: should the logic below mirror the logic for PluginLoadProps?
      Props pluginJobProps = pluginSet.getPluginJobProps(jobType);
      if (pluginJobProps != null) {
        for (String k : pluginJobProps.getKeySet()) {
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
        if(pluginLoadProps == null)
        	pluginLoadProps = new Props();
      }

      job =
          (Job) Utils.callConstructor(executorClass, jobId, pluginLoadProps,
              jobProps, logger);
    } catch (Exception e) {
      logger.error("Failed to build job executor for job " + jobId
          + e.getMessage());
      throw new JobTypeManagerException("Failed to build job executor for job "
          + jobId, e);
    } catch (Throwable t) {
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
