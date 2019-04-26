package azkaban.jobtype;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


// TODO kunkun-tang: This test class needs more refactors.
@Ignore
public class TestHadoopJobUtilsExecutionJar {
  private Logger logger = Logger.getRootLogger();

  private static String currentDirString = System.getProperty("user.dir");
  private static String workingDirString = null;
  private static File workingDirFile = null;
  private static File libFolderFile = null;
  private static File executionJarFile = null;
  private static File libraryJarFile = null;

  @BeforeClass
  public static void setupFolder() {
    workingDirString = currentDirString + "/../temp/TestHadoopSpark";
    workingDirFile = new File(workingDirString);
    libFolderFile = new File(workingDirFile, "lib");
    executionJarFile = new File(libFolderFile,
        "hadoop-spark-job-test-execution-x.y.z-a.b.c"
            + ".jar");
    libraryJarFile = new File(libFolderFile, "library.jar");
  }

  @Before
  public void beforeMethod() throws IOException {
    if (workingDirFile.exists())
      FileUtils.deleteDirectory(workingDirFile);
    workingDirFile.mkdirs();
    libFolderFile.mkdirs();
    executionJarFile.createNewFile();
    libraryJarFile.createNewFile();
  }

  // nothing should happen
  @Test
  public void testNoLibFolder() throws IOException {
    FileUtils.deleteDirectory(libFolderFile);
    String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "lib/*", logger);
    Assert.assertEquals(retval, "");
  }

  // nothing should happen
  @Test
  public void testLibFolderHasNothingInIt() throws IOException {
    FileUtils.deleteDirectory(libFolderFile);
    libFolderFile.mkdirs();
    String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "lib/*", logger);

    Assert.assertEquals(retval, "");
  }


  @Test
  public void testOneLibFolderExpansion() throws IOException {
    String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "lib/*", logger);
    Set<String> retvalSet = new HashSet<String>(Arrays.asList(retval.split(",")));

    Set<String> expected = new HashSet<String>();
    expected.add(workingDirFile + "/lib/library.jar");
    expected.add(workingDirFile + "/lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar");

    Assert.assertTrue("Expected size is different from retrieval size. Expected: " + expected + " , Actual: " + retvalSet,
                      expected.size() == retvalSet.size());
    expected.removeAll(retvalSet);
    Assert.assertTrue("Expected values are not equal to Actual values. Expected: " + expected + " , Actual: " + retvalSet,
                      expected.isEmpty() );
  }

  @Test
  public void testTwoLibFolderExpansionAllFilesResolved() throws IOException {
    File lib2FolderFile = new File(workingDirFile, "lib2");
    lib2FolderFile.mkdirs();
    File lib2test1Jar = new File(lib2FolderFile, "test1.jar");
    lib2test1Jar.createNewFile();
    File lib2test2Jar = new File(lib2FolderFile, "test2.jar");
    lib2test2Jar.createNewFile();
    String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "lib/*,lib2/*",
            logger);

    Assert.assertTrue(retval.contains(workingDirFile + "/lib/library.jar"));
    Assert.assertTrue(retval.contains(workingDirFile + "/lib/hadoop-spark-job-test-execution-x.y"
        + ".z-a.b.c.jar"));
    Assert.assertTrue(retval.contains(workingDirFile + "/lib2/test1.jar"));
    Assert.assertTrue(retval.contains(workingDirFile + "/lib2/test2.jar"));
  }

    @Test
    public void testTwoLibFolderExpansionExpandsInOrder() throws IOException {

      executionJarFile.delete();

      File lib2FolderFile = new File(workingDirFile, "lib2");
      lib2FolderFile.mkdirs();
      File lib2test1Jar = new File(lib2FolderFile, "test1.jar");
      lib2test1Jar.createNewFile();

      String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "lib/*,lib2/*",
              logger);

      Assert.assertEquals(
              retval,
          workingDirFile + "/lib/library.jar," + workingDirFile + "/lib2/test1.jar");
  }
}
