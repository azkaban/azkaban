/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.jobExecutor;

import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.net.URL;
import java.net.URLClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A per-job classloader in which only jobtype plugin classes are loaded.
 * NOTE classes in package azkaban.security and its dependencies, log4j
 * classes and {@link Props} are loaded from its parent classloader.
 */
public class JobClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(JobClassLoader.class);
  private static final String LOG4J_CLASS_PREFIX =
      org.apache.log4j.Logger.class.getPackage().getName();
  private static final String AZKABAN_SECURITY_CLASS = "azkaban.security";
  private final String jobId;
  private final ClassLoader parent;

  public JobClassLoader(final URL[] urls, final ClassLoader parent, final String jobId) {
    super(urls, parent);
    this.parent = parent;
    this.jobId = jobId;
  }

  /**
   * Try to load resources from this classloader's URLs. Note that this is like the servlet spec,
   * not the usual Java behaviour where we ask the parent classloader to attempt to load first.
   */
  @Override
  public URL getResource(final String name) {
    URL url = findResource(name);
    // borrowed from Hadoop, if the resource that starts with '/' is not found, and tries again
    // with the leading '/' removed, in case the resource name was incorrectly specified
    if (url == null && name.startsWith("/")) {
      LOG.debug("Remove leading / off " + name);
      url = findResource(name.substring(1));
    }

    if (url == null) {
      url = this.parent.getResource(name);
    }

    if (url != null) {
      LOG.debug("getResource(" + name + ")=" + url + " for job " + this.jobId);
    }

    return url;
  }

  @Override
  public Class<?> loadClass(final String name) throws ClassNotFoundException {
    return this.loadClass(name, false);
  }

  /**
   * Try to load class from this classloader's URLs. Note that this is like servlet, not the
   * standard behaviour where we ask the parent to attempt to load first.
   */
  @Override
  protected synchronized Class<?> loadClass(final String name, final boolean resolve)
      throws ClassNotFoundException {

    LOG.debug("Loading class: " + name + " for job " + this.jobId);

    Class<?> c = findLoadedClass(name);
    ClassNotFoundException ex = null;

    // A Job instance is instantiated with an instance of Logger and Props loaded from the parent class
    // in JobTypeManager. We must delegate loading of them both to the parent class as such.
    if (c == null && !name.startsWith(LOG4J_CLASS_PREFIX) && !name.equals(Props.class.getName())
        && !name.startsWith(AZKABAN_SECURITY_CLASS)) {
      // if this class has not been loaded before
      try {
        c = findClass(name);
        if (c != null) {
          LOG.debug("Loaded class: " + name + " " + " for job " + this.jobId);
        }
      } catch (final ClassNotFoundException e) {
        LOG.debug(e.toString());
        ex = e;
      }
    }

    // try parent
    if (c == null) {
      c = this.parent.loadClass(name);
      if (c != null) {
        LOG.debug("Loaded class from parent: " + name + " for job " + this.jobId);
      }
    }

    if (c == null) {
      throw ex != null ? ex : new ClassNotFoundException(name);
    }

    if (resolve) {
      // link the specified class as described in the "Execution" chapter of
      // "The Java Language Specification"
      resolveClass(c);
    }

    return c;
  }

  @VisibleForTesting
  void addURL(Class clazz) {
    super.addURL(clazz.getProtectionDomain().getCodeSource().getLocation());
  }
}
