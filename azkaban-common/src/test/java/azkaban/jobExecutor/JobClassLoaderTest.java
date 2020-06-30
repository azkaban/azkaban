package azkaban.jobExecutor;

import static com.google.common.base.Charsets.UTF_8;

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
 * Unit tests for {@link JobClassLoader}.
 */
public class JobClassLoaderTest {

  private final static String RESOURCE_FILE = "resource.txt";
  private static final String SAMPLE_JAR = "helloworld.jar";
  @Rule
  public final TemporaryFolder testDir = new TemporaryFolder();

  /**
   * Test the case where a resource file exists in the Job's classloader.
   */
  @Test
  public void testGetResource() throws IOException {
    final URL testJar = makeTestJar().toURI().toURL();

    final ClassLoader currentClassLoader = getClass().getClassLoader();
    final ClassLoader jobClassLoader = new JobClassLoader(
        new URL[]{testJar}, currentClassLoader, "testJob");

    Assert.assertNull("Resource should not be found in the parent classloader",
        currentClassLoader.getResource(RESOURCE_FILE));
    Assert.assertNotNull("Resource should be found in JobClassLoader",
        jobClassLoader.getResource(RESOURCE_FILE));
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
  public void testNonexistClass() throws ClassNotFoundException {
    final ClassLoader currentClassLoader = getClass().getClassLoader();
    final ClassLoader jobClassLoader = new JobClassLoader(
        new URL[]{}, currentClassLoader, "testJob");

    Assert.assertNull("This class does not exist",
        jobClassLoader.loadClass("nonexistent.class.name"));
  }

  /**
   * Test class loading of a class that is available only in the parent classloader.
   */
  @Test
  public void testClassAvailableInParentClassLoader() throws ClassNotFoundException {
    final ClassLoader currentClassLoader = getClass().getClassLoader();
    final ClassLoader jobClassLoader = new JobClassLoader(
        new URL[]{}, currentClassLoader, "testJob");
    final Class clazz = jobClassLoader.loadClass(JobClassLoaderTest.class.getName());
    Assert.assertEquals(currentClassLoader, clazz.getClassLoader());
  }

  /**
   * Test class loading of a class that is available only in the JobClassloader. The class is
   * provided in 'helloworld.jar'.
   */
  @Test
  public void testClassAvailableInJobClassLoader()
      throws MalformedURLException, ClassNotFoundException {
    final ClassLoader currentClassLoader = getClass().getClassLoader();

    final File helloworldJar = new File(currentClassLoader.getResource(SAMPLE_JAR).getFile());
    final URL helloworlURL = helloworldJar.toURI().toURL();

    final ClassLoader jobClassLoader = new JobClassLoader(
        new URL[]{helloworlURL}, currentClassLoader, "testJob");

    final Class clazz = jobClassLoader.loadClass("org.hello.world.HelloWorld");
    Assert.assertEquals(jobClassLoader, clazz.getClassLoader());
  }
}
