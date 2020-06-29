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


public class PluginUtils {

  private static final Logger logger = LoggerFactory.getLogger(PluginUtils.class);
  private static String LIBRARY_FOLDER_NAME = "lib";

  /**
   * Private constructor.
   */
  private PluginUtils() {
  }

  /**
   * Convert a list of files to a list of files' URLs
   *
   * @param files list of file handles
   * @return an arrayList of the corresponding files' URLs
   */
  private static ArrayList<URL> getUrls(File[] files) {
    final ArrayList<URL> urls = new ArrayList<>();
    for (File file : files) {
      try {
        final URL url = file.toURI().toURL();
        urls.add(url);
      } catch (final MalformedURLException e) {
        logger.error("File is not convertible to URL.", e);
      }
    }
    return urls;
  }

  /**
   * Get URLClassLoader
   */
  public static URLClassLoader getURLClassLoader(final File pluginDir,
      List<String> extLibClassPaths,
      ClassLoader parentLoader) {
    final File libDir = new File(pluginDir, LIBRARY_FOLDER_NAME);
    if (libDir.exists() && libDir.isDirectory()) {
      final File[] files = libDir.listFiles();
      final ArrayList<URL> urls = getUrls(files);

      if (extLibClassPaths != null) {
        for (final String extLibClassPath : extLibClassPaths) {
          try {
            final File extLibFile = new File(pluginDir, extLibClassPath);
            if (extLibFile.exists()) {
              if (extLibFile.isDirectory()) {
                // extLibFile is a directory; load all the files in the
                // directory.
                final File[] extLibFiles = extLibFile.listFiles();
                urls.addAll(getUrls(extLibFiles));
              } else {
                final URL url = extLibFile.toURI().toURL();
                urls.add(url);
              }
            } else {
              logger.error(
                  "External library path not found. path = " + extLibFile.getAbsolutePath()
              );
              continue;
            }
          } catch (final MalformedURLException e) {
            logger.error(
                "Invalid External library path. path = " + extLibClassPath + " dir = " + pluginDir,
                e
            );
          }
        }
      }
      return new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
    } else {
      logger.error("Library path not found. path = " + libDir);
      return null;
    }
  }

  /**
   * Get Plugin Class
   *
   * @param pluginClass plugin class name
   * @param pluginDir plugin root directory
   * @param extLibClassPaths external Library Class Paths
   * @param parentClassLoader parent class loader
   * @return Plugin class or Null
   */
  public static Class<?> getPluginClass(final String pluginClass, final File pluginDir,
      final List<String> extLibClassPaths, ClassLoader parentClassLoader) {

    URLClassLoader urlClassLoader =
        getURLClassLoader(pluginDir, extLibClassPaths, parentClassLoader);

    return getPluginClass(pluginClass, urlClassLoader);
  }

  /**
   * Get Plugin Class
   *
   * @param pluginClass plugin class name
   * @param urlClassLoader url class loader
   * @return Plugin class or Null
   */
  public static Class<?> getPluginClass(final String pluginClass, URLClassLoader urlClassLoader) {

    if (urlClassLoader == null) {
      return null;
    }

    try {
      return urlClassLoader.loadClass(pluginClass);
    } catch (final ClassNotFoundException e) {
      logger.error("Class not found. class = " + pluginClass);
      return null;
    }
  }
}
