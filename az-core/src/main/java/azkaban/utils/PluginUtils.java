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
import org.apache.log4j.Logger;


public class PluginUtils {
  private static final Logger logger = Logger.getLogger(PluginUtils.class);

  /**
   * Private constructor.
   */
  private PluginUtils() {
  }


  public static ArrayList<URL> getUrls(File[] files) {
    final ArrayList<URL> urls = new ArrayList<>();
    for (int i = 0; i < files.length; ++i) {
      try {
        final URL url = files[i].toURI().toURL();
        urls.add(url);
      } catch (final MalformedURLException e) {
        logger.error(e);
      }
    }
    return urls;
  }

  /**
   * Get URLClassLoader
   * @param pluginDir
   * @param extLibClasspath
   * @param parentLoader
   * @return
   */
  public static URLClassLoader getURLClassLoader(final File pluginDir, List<String> extLibClasspath, ClassLoader parentLoader) {
    final File libDir = new File(pluginDir, "lib");
    if (libDir.exists() && libDir.isDirectory()) {
      final File[] files = libDir.listFiles();
      final ArrayList<URL> urls = getUrls(files);

      if (extLibClasspath != null) {
        for (final String extLib : extLibClasspath) {
          try {
            final File extLibFile = new File(pluginDir, extLib);
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
              logger.error("External library path " + extLibFile.getAbsolutePath() + " not found.");
              return null;
            }
          } catch (final MalformedURLException e) {
            logger.error(e);
          }
        }
      }
      return new URLClassLoader(urls.toArray(new URL[urls.size()]), parentLoader);
    } else {
      logger.error("Library path " + libDir + " not found.");
      return null;
    }
  }

  public static Class<?> getPluginClass(final URLClassLoader urlClassLoader, String pluginClass) {
    if (urlClassLoader == null) {
      return null;
    }

    try {
      return urlClassLoader.loadClass(pluginClass);
    } catch (final ClassNotFoundException e) {
      logger.error("Class " + pluginClass + " not found.");
      return null;
    }
  }
}
