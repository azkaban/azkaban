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

import static java.nio.charset.StandardCharsets.UTF_8;

import azkaban.cluster.Cluster.HadoopSecurityManagerClassLoader;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link Cluster}.
 */
public class ClusterTest {
  private final static String HADOOP_SECURITY_MANAGER_CLASS =
      "azkaban.security.HadoopSecurityManager_H_2_0";
  @Rule
  public TemporaryFolder testDir = new TemporaryFolder();

  @Test
  public void testClusterUrlsWithLibraryPathOfJars() throws IOException {
    final Props clusterProps = new Props();
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);
    final File hadoopJar = makeTestJar("hadoop", "hadoop.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hadoop", hadoopJar.getPath());
    final File hiveJar = makeTestJar("hive", "hive.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hive", hiveJar.getPath());
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);

    final Cluster cluster = new Cluster("default", clusterProps);

    final List<URL> clusterUrls = cluster.getClusterComponentURLs(Arrays.asList("hive", "hadoop"));
    final Set<String> clusterUrlFiles = new HashSet<>();
    for (final URL url : clusterUrls) {
      clusterUrlFiles.add(url.getFile());
    }

    Assert.assertTrue(clusterUrlFiles.contains(hiveJar.getPath()));
    Assert.assertTrue(clusterUrlFiles.contains(hadoopJar.getPath()));
    Assert.assertTrue(clusterUrls.size() == 2);
  }

  @Test
  public void testClusterUrlsWithLibraryPathOfDirectories() throws IOException {
    final Props clusterProps = new Props();
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);
    final File hadoopJar = makeTestJar("hadoop", "hadoop.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hadoop", hadoopJar.getParentFile().getPath());
    final File hiveJar = makeTestJar("hive", "hive.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hive", hiveJar.getParentFile().getPath());
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);

    final Cluster cluster = new Cluster("default", clusterProps);

    final List<URL> clusterUrls = cluster.getClusterComponentURLs(Arrays.asList("hive", "hadoop"));
    final Set<String> clusterUrlFiles = new HashSet<>();
    for (final URL url : clusterUrls) {
      clusterUrlFiles.add(url.getFile());
    }

    Assert.assertTrue(clusterUrlFiles.contains(hiveJar.getParentFile().getPath() + "/"));
    Assert.assertTrue(clusterUrlFiles.contains(hiveJar.getPath()));
    Assert.assertTrue(clusterUrlFiles.contains(hadoopJar.getParentFile().getPath() + "/"));
    Assert.assertTrue(clusterUrlFiles.contains(hadoopJar.getPath()));
    Assert.assertTrue(clusterUrls.size() == 4);
  }

  @Test
  public void testClusterUrlsWithLibraryPathOfWildcards() throws IOException {
    final Props clusterProps = new Props();
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);
    final File hadoopJar = makeTestJar("hadoop", "hadoop.jar");
    clusterProps
        .put(Cluster.LIBRARY_PATH_PREFIX + "hadoop", hadoopJar.getParentFile().getPath() + "/*");
    final File hiveJar = makeTestJar("hive", "hive.jar");
    clusterProps
        .put(Cluster.LIBRARY_PATH_PREFIX + "hive", hiveJar.getParentFile().getPath() + "/*");
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);

    final Cluster cluster = new Cluster("default", clusterProps);

    final List<URL> clusterUrls = cluster.getClusterComponentURLs(Arrays.asList("hive", "hadoop"));
    final Set<String> clusterUrlFiles = new HashSet<>();
    for (final URL url : clusterUrls) {
      clusterUrlFiles.add(url.getFile());
    }

    Assert.assertTrue(clusterUrlFiles.contains(hiveJar.getParentFile().getPath() + "/"));
    Assert.assertTrue(clusterUrlFiles.contains(hiveJar.getPath()));
    Assert.assertTrue(clusterUrlFiles.contains(hadoopJar.getParentFile().getPath() + "/"));
    Assert.assertTrue(clusterUrlFiles.contains(hadoopJar.getPath()));
    Assert.assertTrue(clusterUrls.size() == 4);
  }

  @Test
  public void testClusterUrlsOfSubsetOfComponentsWithLibraryPathOfWildcards()
      throws IOException {
    final Props clusterProps = new Props();
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);
    final File hadoopJar = makeTestJar("hadoop", "hadoop.jar");
    clusterProps
        .put(Cluster.LIBRARY_PATH_PREFIX + "hadoop", hadoopJar.getParentFile().getPath() + "/*");
    final File hiveJar = makeTestJar("hive", "hive.jar");
    clusterProps
        .put(Cluster.LIBRARY_PATH_PREFIX + "hive", hiveJar.getParentFile().getPath() + "/*");
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);

    final Cluster cluster = new Cluster("default", clusterProps);

    final List<URL> clusterUrls = cluster.getClusterComponentURLs(Arrays.asList("hive"));
    final Set<String> clusterUrlFiles = new HashSet<>();
    for (final URL url : clusterUrls) {
      clusterUrlFiles.add(url.getFile());
    }

    Assert.assertTrue(clusterUrlFiles.contains(hiveJar.getParentFile().getPath() + "/"));
    Assert.assertTrue(clusterUrlFiles.contains(hiveJar.getPath()));
    Assert.assertTrue(clusterUrls.size() == 2);
  }
  @Test (expected = IllegalArgumentException.class)
  public void testClusterUrlsOfNonexistentComponentsWithLibraryPathOfWildcards()
      throws IOException {
    final Props clusterProps = new Props();
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);
    final File hadoopJar = makeTestJar("hadoop", "hadoop.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hadoop", hadoopJar.getParentFile().getPath() + "/*");
    final File hiveJar = makeTestJar("hive", "hive.jar");
    clusterProps
        .put(Cluster.LIBRARY_PATH_PREFIX + "hive", hiveJar.getParentFile().getPath() + "/*");
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);

    final Cluster cluster = new Cluster("default", clusterProps);

    // pig is not one of the components in the default cluster
    final List<URL> clusterUrls = cluster.getClusterComponentURLs(Arrays.asList("pig"));
  }

  private File makeTestJar(final String folderName, final String jarName) throws IOException {
    final File folder = this.testDir.newFolder(folderName);
    final File jarFile = new File(folder, jarName);
    try (final JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile))) {
      final ZipEntry entry = new ZipEntry("resource.txt");
      out.putNextEntry(entry);
      out.write("hello".getBytes(UTF_8));
      out.closeEntry();
    }
    return jarFile;
  }

  @Test
  public void testJavaLibraryPathAsString() throws IOException {
    final Props clusterProps = new Props();
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);
    final File hadoopJar = makeTestJar("hadoop", "hadoop.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hadoop", hadoopJar.getParentFile().getPath());
    final File hiveJar = makeTestJar("hive", "hive.jar");
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hive", hiveJar.getParentFile().getPath());
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);

    final Cluster cluster = new Cluster("default", clusterProps);

    final String[] libraryPaths = cluster.getJavaLibraryPath(Arrays.asList("hadoop", "hive"))
        .split(Cluster.PATH_DELIMITER);
    final Set<String> javaLibraryPaths = new HashSet<>(Arrays.asList(libraryPaths));

    Assert.assertTrue(javaLibraryPaths.contains(hadoopJar.getParentFile().getPath()));
    Assert.assertTrue(javaLibraryPaths.contains(hiveJar.getParentFile().getPath()));
  }

  @Test
  public void testNativeLibraryPathAsString() throws IOException {
    final Props clusterProps = new Props();
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);
    clusterProps.put(Cluster.NATIVE_LIBRARY_PATH_PREFIX + "hadoop", "hadoop-native-library/");
    clusterProps.put(Cluster.NATIVE_LIBRARY_PATH_PREFIX + "hive", "hive-native-library/");
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, HADOOP_SECURITY_MANAGER_CLASS);

    final Cluster cluster = new Cluster("default", clusterProps);

    final String[] libraryPaths = cluster.getNativeLibraryPath(Arrays.asList("hadoop", "hive"))
        .split(Cluster.PATH_DELIMITER);
    final Set<String> javaLibraryPaths = new HashSet<>(Arrays.asList(libraryPaths));

    Assert.assertTrue(javaLibraryPaths.contains("hive-native-library/"));
    Assert.assertTrue(javaLibraryPaths.contains("hadoop-native-library/"));
  }

  /**
   * Test loading of the HadoopSecurityManager class figured for a given cluster
   * from its dedicated HadoopSecurityManagerClassLoader instance.
   */
  @Test
  public void testGetClusterSecurityManager() throws ClassNotFoundException {
    final String fakeHadoopSecurityManagerClassName = "org.hello.world.HelloWorld";
    final Props clusterProps = new Props();
    final File hadoopSecurityManagerJar =
        new File(getClass().getClassLoader().getResource("helloworld.jar").getFile());
    clusterProps.put(Cluster.LIBRARY_PATH_PREFIX + "hadoopsecuritymanager",
        hadoopSecurityManagerJar.getParentFile().getPath());
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_DEPENDENCY_COMPONENTS, "hadoopsecuritymanager");
    clusterProps.put(Cluster.HADOOP_SECURITY_MANAGER_CLASS_PROP, fakeHadoopSecurityManagerClassName);

    final Cluster cluster = new Cluster("default", clusterProps);
    final HadoopSecurityManagerClassLoader classLoader = cluster.getSecurityManagerClassLoader();

    classLoader.loadClass(fakeHadoopSecurityManagerClassName);
  }
}
