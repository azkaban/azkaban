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
package azkaban.cluster;

import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A in-memory representation of a Hadoop cluster loaded by {@link ClusterLoader} for each directory
 * where there is a cluster.properties file.
 *
 * The id of a cluster is determined by the name of the its corresponding directory (so we can
 * ensure its uniqueness) and its properties are loaded from its cluster.properties file.
 *
 * There are three kinds of information that can be found it a cluster's cluster.properties:
 * 1) hadoop.security.manager.class that specifies which implementation of HadoopSecurityManager
 * to use to manage job tokens for a given Hadoop cluster. In theory, each version of a cluster
 * can have a slightly different implementation.
 * 2) java libraries and native libraries for each component that is available on the Hadoop cluster
 * The standard java classpath (-cp) format is used. The URLs of the java libraries for each cluster
 * component are loaded on demand.
 * 3) Other cluster specific configurations, such as tuning.api.end.point that specified where
 * the API endpoint is to auto-tune Spark jobs. There properties are exposed to individual jobs
 * as part of their job properties.
 */
public class Cluster {
  private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

  public static final Cluster UNKNOWN = new Cluster("UNKNOWN", new Props());
  public static final String DEFAULT_CLUSTER = "default";
  public static final String PATH_DELIMITER = ":";

  public static final String HADOOP_SECURITY_MANAGER_CLASS_PROP =
      "hadoop.security.manager.class";
  public static final String NATIVE_LIBRARY_PATH_PREFIX =
      "native.library.path.";
  public static final String LIBRARY_PATH_PREFIX = "library.path.";
  // a comma list of jar patterns that can be used to exclude problematic libraries from
  // being exposed to jobs. For example, xml-apis-*.jar can triggers an attempt to load
  // org.apache.xalan.processor.TransformerFactoryImpl, a behavior conflicts with JDK.
  public static final String EXCLUDED_LIBRARY_PATTERNS = "library.excluded.patterns";

  public static final String HADOOP_SECURITY_MANAGER_DEPENDENCY_COMPONENTS =
      "hadoop.security.manager.dependency.components";

  public final String clusterId;
  private final Props properties;
  private final List<Pattern> excludedLibraryPatterns;
  private final Map<String, List<URL>> componentURLs = new ConcurrentHashMap<>();
  private volatile HadoopSecurityManagerClassLoader securityManagerClassLoader;

  public Cluster(final String clusterId, final Props properties) {
    this.clusterId = clusterId;
    this.properties = properties;

    final List<String> exclusionPatterns = properties.getStringList(EXCLUDED_LIBRARY_PATTERNS);
    this.excludedLibraryPatterns = new ArrayList<>(exclusionPatterns.size());
    for (final String exclusion : exclusionPatterns) {
      this.excludedLibraryPatterns.add(Pattern.compile(exclusion));
    }
  }

  public String getClusterId() {
    return clusterId;
  }

  public Props getProperties() {
    return properties;
  }

  public HadoopSecurityManagerClassLoader getSecurityManagerClassLoader() {
    if (this.securityManagerClassLoader == null) {
      synchronized(this) {
        if (this.securityManagerClassLoader == null) {
          final List<String> hadoopSecurityManagerDependencyComponents = this.properties.getStringList(
              HADOOP_SECURITY_MANAGER_DEPENDENCY_COMPONENTS);
          final List<URL> clusterUrls;
          try {
            clusterUrls = getClusterComponentURLs(hadoopSecurityManagerDependencyComponents);
          } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                String.format("Invalid dependency components for " +
                    HadoopSecurityManagerClassLoader.class.getName() + " of cluster %s", clusterId));
          }
          final URL[] urls = new URL[clusterUrls.size()];
          clusterUrls.toArray(urls);
          this.securityManagerClassLoader =
              new HadoopSecurityManagerClassLoader(urls, Cluster.class.getClassLoader(), clusterId);
          LOGGER.info(String.format(HadoopSecurityManagerClassLoader.class.getName() + " for "
                  + "cluster %s is loaded with URLs:  %s", clusterId,
              clusterUrls.stream().map(URL::toString).collect(Collectors.joining(", "))));
        }
      }
    }

    return this.securityManagerClassLoader;
  }

  @Override
  public String toString() {
    return String.format("cluster: %s with properties %s", this.clusterId, this.properties);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Cluster)) {
      return false;
    }
    final Cluster other = (Cluster) obj;

    return Objects.equals(other.clusterId, this.clusterId) &&
        Objects.equals(other.properties, this.properties) &&
        Objects.equals(this.excludedLibraryPatterns, other.excludedLibraryPatterns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.clusterId, this.properties, this.excludedLibraryPatterns);
  }

  /**
   * Get library URLs for a given set of components.
   */
  public List<URL> getClusterComponentURLs(final Collection<String> components)
      throws MalformedURLException {
    final List<URL> urls = new ArrayList<>();
    final Map<String, String> componentClasspath = this.properties.getMapByPrefix(
        Cluster.LIBRARY_PATH_PREFIX);
    for (final String component : components) {
      if (!this.componentURLs.containsKey(component)) {
        final String classpath = componentClasspath.get(component);
        if (classpath == null) {
          throw new IllegalArgumentException(
              String.format("Could not find component: %s from cluster: %s", component,
                  this.clusterId));
        }
        this.componentURLs.putIfAbsent(component, getResourcesFromClasspath(classpath));
      }
      urls.addAll(this.componentURLs.get(component));
    }
    return urls;
  }

  private List<URL> getResourcesFromClasspath(final String clusterLibraryPath)
      throws MalformedURLException {
    final List<URL> resources = new ArrayList<>();

    if (Strings.isNullOrEmpty(clusterLibraryPath)) {
      return resources;
    }

    for (String path : clusterLibraryPath.split(PATH_DELIMITER)) {
      final File file = new File(path);
      if (file.isFile()) {
        if (!shouldBeExcluded(file)) {
          resources.add(file.toURI().toURL());
        }
      } else {
        // strip the trailing * character from the path
        path = path.replaceAll("\\*$", "");
        final File folder = new File(path);
        if (folder.exists()) {
          resources.add(folder.toURI().toURL());
          for (final File jar : folder.listFiles()) {
            if (jar.getName().endsWith(".jar") && !shouldBeExcluded(jar)) {
              resources.add(jar.toURI().toURL());
            }
          }
        }
      }
    }

    return resources;
  }

  /**
   * Check whether a given file should not be loaded as part of cluster component libraries.
   *
   * For example, xml-apis-*.jar in Hadoop library includes a copy of
   * javax/xml/transform/TransformerFactory.class that conflicts with the version from JDK.
   * The one in xml-apis-*.jar triggers an attempt to load org.apache.xalan.processor
   * .TransformerFactoryImpl which cannot be found in classpath.
   *
   * Problematic libraries can be excluded from individual jobs' classpath by configuring
   * library.excluded.patterns for each cluster.
   *
   */
  private boolean shouldBeExcluded(final File file) {
    if (file == null) {
      return true;
    }
    final String fileName = file.getName();
    for (final Pattern exclude : this.excludedLibraryPatterns) {
      if (exclude.matcher(fileName).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the native library path as a String to be provided through -Djava.library.path to job JVM
   * process.
   */
  public String getNativeLibraryPath(final Collection<String> components) {
    return getNativeLibraryPath(this.properties, components);
  }

  public static String getNativeLibraryPath(final Props properties,
      final Collection<String> components) {
    final List<String> nativeLibraryLibPaths = new ArrayList<>();
    final Map<String, String> compoNativeLibPaths = properties.getMapByPrefix(
        Cluster.NATIVE_LIBRARY_PATH_PREFIX);
    for (final String component : components) {
      final String nativeLibPath = compoNativeLibPaths.get(component);
      if (nativeLibPath != null) {
        nativeLibraryLibPaths.add(nativeLibPath);
      }
    }
    return nativeLibraryLibPaths.isEmpty() ? Strings.EMPTY
        : String.join(PATH_DELIMITER, nativeLibraryLibPaths);
  }

  /**
   * Get library paths for a given set of components as a ':' separated string.
   */
  public String getJavaLibraryPath(final Collection<String> components) {
    return getJavaLibraryPath(this.properties, components);
  }

  public static String getJavaLibraryPath(final Props properties,
      final Collection<String> components) {
    final List<String> classpaths = new ArrayList<>();
    final Map<String, String> componentClasspaths = properties.getMapByPrefix(
        Cluster.LIBRARY_PATH_PREFIX);
    for (final String component : components) {
      final String libPath = componentClasspaths.get(component);
      if (libPath != null) {
        classpaths.add(libPath);
      }
      if (libPath == null) {
        throw new IllegalArgumentException(
            String.format("Could not find libraries for component: %s ", component));
      }
    }
    return classpaths.isEmpty() ? Strings.EMPTY : String.join(PATH_DELIMITER, classpaths);
  }

  /**
   * A per-cluster classloader for HadoopSecurityManager.
   */
  public static class HadoopSecurityManagerClassLoader extends URLClassLoader {
    private final static Logger LOG = LoggerFactory.getLogger(HadoopSecurityManagerClassLoader.class);

    private static final String LOG4J_CLASS_PREFIX =
        Logger.class.getPackage().getName();

    private final ClassLoader parent;

    static {
       if (!ClassLoader.registerAsParallelCapable()) {
          LOG.warn("HadoopSecurityManagerClassLoader's request of registering as parallel capable"
              + " failed.");
       }
    }

    public HadoopSecurityManagerClassLoader(URL[] urls, ClassLoader parent, String clusterId) {
      super(urls, Cluster.class.getClassLoader());
      this.parent = Cluster.class.getClassLoader();
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
        LOG.debug("getResource(" + name + ")=" + url);
      }

      return url;
    }

    /**
     * Try to load class from this classloader's URLs. Note that this is like servlet, not the
     * standard behaviour where we ask the parent to attempt to load first.
     */
    @Override
    protected Class<?> loadClass(final String name, final boolean resolve)
        throws ClassNotFoundException {

      synchronized (getClassLoadingLock(name)) {
        Class<?> c = findLoadedClass(name);
        ClassNotFoundException ex = null;

        if (c == null && !name.startsWith(LOG4J_CLASS_PREFIX) && !name.equals(Props.class.getName())) {
          // if this class has not been loaded before
          try {
            c = findClass(name);
            if (c != null) {
              LOG.debug("Loaded class: " + name);
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
            LOG.debug("Loaded class from parent: " + name);
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

    }

    @VisibleForTesting
    void addURL(Class clazz) {
      super.addURL(clazz.getProtectionDomain().getCodeSource().getLocation());
    }
  }
}
