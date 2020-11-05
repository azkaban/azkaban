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

import static com.google.common.base.Charsets.UTF_8;

import azkaban.cluster.Cluster.HadoopSecurityManagerClassLoader;
import azkaban.utils.Props;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link azkaban.cluster.Cluster.HadoopSecurityManagerClassLoader}.
 */
public class HadoopSecurityManagerClassLoaderTest {

  private final static String RESOURCE_FILE = "resource.txt";
  private static final String SAMPLE_JAR = "helloworld.jar";
  @Rule
  public final TemporaryFolder testDir = new TemporaryFolder();

  /**
   * Test the case where a resource file exists in the HadoopSecurityManager classloader.
   */
  @Test
  public void testGetResource() throws IOException {
    final URL testJar = makeTestJar().toURI().toURL();

    final ClassLoader currentClassLoader = getClass().getClassLoader();
    final ClassLoader hadoopSecurityManagerClassLoader =
        new HadoopSecurityManagerClassLoader(
            new URL[]{testJar}, currentClassLoader, "testCluster");

    Assert.assertNull("Resource should not be found in the parent classloader",
        currentClassLoader.getResource(RESOURCE_FILE));
    Assert.assertNotNull("Resource should be found in HadoopSecurityManagerClassLoader",
        hadoopSecurityManagerClassLoader.getResource(RESOURCE_FILE));
  }

  private File makeTestJar() throws IOException {
    final File jarFile = this.testDir.newFile("test.jar");
    try (final JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile))) {
      final ZipEntry entry = new ZipEntry(RESOURCE_FILE);
      out.putNextEntry(entry);
      out.write("hello".getBytes(UTF_8));
      out.closeEntry();
    }
    return jarFile;
  }

  /**
   * Test the case where the define job class does not exist.
   */
  @Test(expected = ClassNotFoundException.class)
  public void testNonExistClass() throws ClassNotFoundException {
    final ClassLoader currentClassLoader = getClass().getClassLoader();
    final ClassLoader hadoopSecurityManagerClassLoader =
        new HadoopSecurityManagerClassLoader(
            new URL[]{}, currentClassLoader, "testCluster");

    Assert.assertNull("This class does not exist",
        hadoopSecurityManagerClassLoader.loadClass("nonexistent.class.name"));
  }

  /**
   * Test class loading of a class that is available only in the parent classloader.
   */
  @Test
  public void testClassAvailableInParentClassLoader() throws ClassNotFoundException {
    final ClassLoader currentClassLoader = getClass().getClassLoader();
    final ClassLoader hadoopSecurityManagerClassLoader =
        new HadoopSecurityManagerClassLoader(
            new URL[]{}, currentClassLoader, "testCluster");
    final Class clazz = hadoopSecurityManagerClassLoader.loadClass(
        HadoopSecurityManagerClassLoaderTest.class.getName());
    Assert.assertEquals(currentClassLoader, clazz.getClassLoader());
  }

  /**
   * Test class loading of a class that is available only in the HadoopSecurityManagerClassLoader.
   * The class is provided in 'helloworld.jar'.
   */
  @Test
  public void testClassAvailableInHadoopSecurityManagerClassLoader()
      throws MalformedURLException, ClassNotFoundException {
    final ClassLoader currentClassLoader = getClass().getClassLoader();

    final File helloworldJar = new File(currentClassLoader.getResource(SAMPLE_JAR).getFile());
    final URL helloworlURL = helloworldJar.toURI().toURL();

    final ClassLoader hadoopSecurityManagerClassLoader =
        new HadoopSecurityManagerClassLoader(
            new URL[]{helloworlURL}, currentClassLoader, "testCluster");

    final Class clazz = hadoopSecurityManagerClassLoader.loadClass(
        "org.hello.world.HelloWorld");
    Assert.assertEquals(hadoopSecurityManagerClassLoader, clazz.getClassLoader());
  }

  /**
   * Check {@link org.apache.log4j.Logger} is always loaded by its parent classloader.
   */
  @Test
  public void testLog4JClass() throws ClassNotFoundException {
    ClassLoader currentClassLoader = getClass().getClassLoader();
    HadoopSecurityManagerClassLoader hadoopSecurityManagerClassLoader =
        new HadoopSecurityManagerClassLoader(
            new URL[] {}, currentClassLoader, "testCluster");
    // make org.apache.log4j.Logger class available to the HadoopSecurityManagerClassLoader
    hadoopSecurityManagerClassLoader.addURL(org.apache.log4j.Logger.class);

    Class clazz = hadoopSecurityManagerClassLoader.loadClass(
        org.apache.log4j.Logger.class.getName());
    Assert.assertEquals(currentClassLoader, clazz.getClassLoader());
  }

  /**
   * Check {@link azkaban.utils.Props} is always loaded by its parent classloader.
   */
  @Test
  public void testPropsClass() throws ClassNotFoundException {
    ClassLoader currentClassLoader = getClass().getClassLoader();
    HadoopSecurityManagerClassLoader hadoopSecurityManagerClassLoader =
        new HadoopSecurityManagerClassLoader(
            new URL[] {}, currentClassLoader, "testCluster");
    // make azkaban.utils.Props class available to the HadoopSecurityManagerClassLoader
    hadoopSecurityManagerClassLoader.addURL(Props.class);

    Class clazz = hadoopSecurityManagerClassLoader.loadClass(Props.class.getName());
    Assert.assertEquals(currentClassLoader, clazz.getClassLoader());
  }
}
