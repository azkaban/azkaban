/*
 * Copyright 2019 LinkedIn Corp.
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
package azkaban.utils;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dependency Injection Utilities
 */
public class DIUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(DIUtils.class);

  /**
   * Get ClassLoader From Resources
   *
   * @param resources list of resource
   * @param parent parent class loader
   * @return class loader
   */
  public static ClassLoader getClassLoader(List<URL> resources, ClassLoader parent) {
    return new URLClassLoader(resources.toArray(new URL[resources.size()]), parent);
  }

  /**
   * Load Setting to Resources
   *
   * @param pluginDir plugin Directory
   * @param globalClassPaths list of global class path
   * @param classPaths list of local class path
   * @param libDirectories list of lib directories
   * @return
   */
  public static List<URL> loadResources(
      final File pluginDir,
      final List<String> globalClassPaths,
      final List<String> classPaths,
      final List<String> libDirectories
  ) throws MalformedURLException {
    final List<URL> resources = new ArrayList<>();

    LOGGER.info("Adding global resources");
    loadResources(globalClassPaths, resources);

    LOGGER.info("Adding type resources");
    loadResources(classPaths, resources);

    LOGGER.info("Adding lib resources");
    if (libDirectories != null) {
      for (final String libDir : libDirectories) {
        loadResources(new File(libDir), resources);
      }
    }

    LOGGER.info("Adding type override resources");
    loadResources(pluginDir, resources);

    return resources;
  }

  /**
   * Load Setting from a list of class paths to Resources
   *
   * @param classPaths list of class paths
   * @param resources list of resource
   * @throws MalformedURLException
   */
  private static void loadResources(final List<String> classPaths, List<URL> resources) throws MalformedURLException {
    if (classPaths != null) {
      for (final String jar : classPaths) {
        final URL cpItem = new File(jar).toURI().toURL();
        if (!resources.contains(cpItem)) {
          LOGGER.info("adding to classpath " + cpItem);
          resources.add(cpItem);
        }
      }
    }
  }

  /**
   * Load Setting from file directory to Resources
   *
   * @param directory file directory for .jar files
   * @param resources list of resource
   * @throws MalformedURLException
   */
  private static void loadResources(final File directory, List<URL> resources) throws MalformedURLException {
    for (final File file : directory.listFiles()) {
      if (file.getName().endsWith(".jar")) {
        resources.add(file.toURI().toURL());
        LOGGER.info("adding to classpath " + file.toURI().toURL());
      }
    }
  }
}
