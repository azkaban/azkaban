/*
 * Copyright 2017 LinkedIn Corp.
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
package azkaban.flowtrigger.plugin;

import azkaban.flowtrigger.DependencyCheck;
import azkaban.flowtrigger.DependencyPluginConfig;
import azkaban.flowtrigger.DependencyPluginConfigImpl;
import azkaban.utils.Utils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class FlowTriggerDependencyPluginManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(FlowTriggerDependencyPluginManager.class);
  public static final String CONFIG_FILE = "dependency.properties";
  public static final String PRIVATE_CONFIG_FILE = "private.properties";
  public static final String DEPENDENCY_CLASS = "dependency.class";
  public static final String CLASS_PATH = "dependency.classpath";
  private final String pluginDir;
  private final Map<String, DependencyCheck> dependencyTypeMap;
  private final ClassLoader prevClassLoader;

  @Inject
  public FlowTriggerDependencyPluginManager(final String pluginDir)
      throws FlowTriggerDependencyPluginException {
    this.dependencyTypeMap = new ConcurrentHashMap<>();
    this.pluginDir = pluginDir;
    this.prevClassLoader = Thread.currentThread().getContextClassLoader();
  }

  /**
   * retrieve files with wildcard matching.
   * Only support "dir/*". Pattern like "dir/foo*" or "dir/*foo*" will not be supported
   * since user shouldn't upload the jars they don't want to import
   * the reason for supporting dir/* is to provide another packaging option
   * which let user upload a dir of all required jars
   * in addition to one fat jar.
   */
  private File[] getFilesMatchingPath(final String path) {
    if (path.endsWith("*")) {
      final File dir = new File(path.substring(0, path.lastIndexOf("/") + 1));
      final FileFilter fileFilter = new WildcardFileFilter(path.substring(path.lastIndexOf("/")
          + 1));
      final File[] files = dir.listFiles(fileFilter);
      return files;
    } else {
      return new File[]{new File(path)};
    }
  }

  private Map<String, String> readConfig(final File file) throws
      FlowTriggerDependencyPluginException {
    final Properties props = new Properties();
    InputStream input = null;
    try {
      input = new BufferedInputStream(new FileInputStream(file));
      props.load(input);
    } catch (final Exception e) {
      LOG.debug("unable to read the file " + file, e);
      throw new FlowTriggerDependencyPluginException(e);
    } finally {
      try {
        if (input != null) {
          input.close();
        }
      } catch (final IOException e) {
        LOG.error("unable to close input stream when reading config from file " + file
            .getAbsolutePath(), e);
      }
    }
    return Maps.fromProperties(props);
  }

  private void validatePluginConfig(final DependencyPluginConfig pluginConfig)
      throws FlowTriggerDependencyPluginException {
    for (final String requiredField : ImmutableSet
        .of(DEPENDENCY_CLASS, CLASS_PATH)) {
      if (StringUtils.isEmpty(pluginConfig.get(requiredField))) {
        throw new FlowTriggerDependencyPluginException("missing " + requiredField + " in "
            + "dependency plugin properties");
      }
    }
  }

  private DependencyPluginConfig mergePluginConfig(final Map<String, String> publicProps,
      final Map<String, String> privateProps) throws FlowTriggerDependencyPluginException {
    final Map<String, String> combined = new HashMap<>();
    combined.putAll(publicProps);
    combined.putAll(privateProps);
    if (combined.size() != publicProps.size() + privateProps.size()) {
      throw new FlowTriggerDependencyPluginException("duplicate property found in both public and"
          + " private properties");
    }
    return new DependencyPluginConfigImpl(combined);
  }

  private DependencyCheck createDependencyCheck(final DependencyPluginConfig pluginConfig)
      throws FlowTriggerDependencyPluginException {
    final String classPath = pluginConfig.get(CLASS_PATH);

    final String[] cpList = classPath.split(",");

    final List<URL> resources = new ArrayList<>();

    try {
      for (final String cp : cpList) {
        final File[] files = getFilesMatchingPath(cp);
        if (files != null) {
          for (final File file : files) {
            final URL cpItem = file.toURI().toURL();
            if (!resources.contains(cpItem)) {
              LOG.info("adding to classpath " + cpItem);
              resources.add(cpItem);
            }
          }
        }
      }
    } catch (final Exception ex) {
      throw new FlowTriggerDependencyPluginException(ex);
    }

    final ClassLoader dependencyClassloader = new ParentLastURLClassLoader(
        resources.toArray(new URL[resources.size()]), this.getClass().getClassLoader());

    Thread.currentThread().setContextClassLoader(dependencyClassloader);

    Class<? extends DependencyCheck> clazz = null;
    try {
      clazz = (Class<? extends DependencyCheck>) dependencyClassloader.loadClass(pluginConfig.get
          (DEPENDENCY_CLASS));
      return (DependencyCheck) Utils.callConstructor(clazz);
    } catch (final Exception ex) {
      throw new FlowTriggerDependencyPluginException(ex);
    }
  }

  public void loadDependencyPlugin(final File pluginDir)
      throws FlowTriggerDependencyPluginException {
    if (pluginDir.isDirectory() && pluginDir.canRead()) {
      try {
        final DependencyPluginConfig pluginConfig = createPluginConfig(pluginDir);
        final DependencyCheck depCheck = createDependencyCheck(pluginConfig);
        final String pluginName = getPluginName(pluginDir);
        depCheck.init(pluginConfig);
        this.dependencyTypeMap.put(pluginName, depCheck);
      } catch (final Exception ex) {
        LOG.error("failed to initializing plugin in " + pluginDir, ex);
        throw new FlowTriggerDependencyPluginException(ex);
      }
    }
  }

  /**
   * Initialize all dependency plugins.
   * todo chengren311: Current design aborts loadAllPlugins if any of the plugin fails to be
   * initialized.
   * However, this might not be the optimal design. Suppose we have two dependency plugin types
   * - MySQL and Kafka, if MySQL is down, then kafka dependency type will also be unavailable.
   */
  public void loadAllPlugins() throws FlowTriggerDependencyPluginException {
    final File pluginDir = new File(this.pluginDir);
    for (final File dir : pluginDir.listFiles()) {
      loadDependencyPlugin(dir);
    }
    //reset thread context loader so that other azkaban class will be loaded with the old
    // classloader
    Thread.currentThread().setContextClassLoader(this.prevClassLoader);
  }

  private String getPluginName(final File dependencyPluginDir) {
    //the name of the dependency plugin dir is treated as the name of the plugin
    return dependencyPluginDir.getName();
  }

  private Map<String, String> readPublicConfig(final File publicConfigFile)
      throws FlowTriggerDependencyPluginException {
    return readConfig(publicConfigFile);
  }

  /**
   * read config from private property file, if the file is not present, then return empty.
   */
  private Map<String, String> readPrivateConfig(final File privateConfigFile) {
    try {
      return readConfig(privateConfigFile);
    } catch (final Exception ex) {
      return new HashMap<>();
    }
  }

  private DependencyPluginConfig createPluginConfig(final File dir) throws
      FlowTriggerDependencyPluginException {
    final File publicConfigFile = new File(dir.getAbsolutePath() + "/" + CONFIG_FILE);
    final File privateConfigFile = new File(dir.getAbsolutePath() + "/" + PRIVATE_CONFIG_FILE);
    try {
      final DependencyPluginConfig pluginConfig = mergePluginConfig(
          readPublicConfig(publicConfigFile),
          readPrivateConfig(privateConfigFile));
      validatePluginConfig(pluginConfig);
      return pluginConfig;
    } catch (final FlowTriggerDependencyPluginException exception) {
      throw new FlowTriggerDependencyPluginException("exception when initializing plugin "
          + "config in " + dir.getAbsolutePath() + ": " + exception.getMessage());
    }
  }

  /**
   * return or create a dependency check based on type
   *
   * @return if the dependencyCheck of the same type already exists, return the check,
   * otherwise create a new one and return.
   */

  public DependencyCheck getDependencyCheck(final String type) {
    return this.dependencyTypeMap.get(type);
  }

  public void shutdown() {
    for (final DependencyCheck depCheck : this.dependencyTypeMap.values()) {
      try {
        depCheck.shutdown();
      } catch (final Exception ex) {
        LOG.error("failed to shutdown dependency check " + depCheck, ex);
      }
    }
  }
}
