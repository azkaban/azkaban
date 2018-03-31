package jobtype;

import azkaban.jobtype.HadoopJobUtils;
import azkaban.jobtype.SparkJobArg;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.utils.Props;

public class TestHadoopJobUtilsExecutionJar {
  Props jobProps = null;

  Logger logger = Logger.getRootLogger();

  String workingDirString = "/tmp/TestHadoopSpark";

  File workingDirFile = new File(workingDirString);

  File libFolderFile = new File(workingDirFile, "lib");

  String executionJarName = "hadoop-spark-job-test-execution-x.y.z-a.b.c.jar";

  File executionJarFile = new File(libFolderFile, "hadoop-spark-job-test-execution-x.y.z-a.b.c.jar");

  File libraryJarFile = new File(libFolderFile, "library.jar");

  String delim = SparkJobArg.delimiter;

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
    String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "./lib/*", logger);

    Assert.assertEquals(retval, "");
  }

  // nothing should happen
  @Test
  public void testLibFolderHasNothingInIt() throws IOException {
    FileUtils.deleteDirectory(libFolderFile);
    libFolderFile.mkdirs();
    String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "./lib/*", logger);

    Assert.assertEquals(retval, "");
  }


  @Test
  public void testOneLibFolderExpansion() throws IOException {
    String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "./lib/*", logger);
    Set<String> retvalSet = new HashSet<String>(Arrays.asList(retval.split(",")));

    Set<String> expected = new HashSet<String>();
    expected.add("/tmp/TestHadoopSpark/./lib/library.jar");
    expected.add("/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar");

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
    String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "./lib/*,./lib2/*",
            logger);

    Assert.assertTrue(retval.contains("/tmp/TestHadoopSpark/./lib/library.jar"));
    Assert.assertTrue(retval.contains("/tmp/TestHadoopSpark/./lib/hadoop-spark-job-test-execution-x.y.z-a.b.c.jar"));
    Assert.assertTrue(retval.contains("/tmp/TestHadoopSpark/./lib2/test1.jar"));
    Assert.assertTrue(retval.contains("/tmp/TestHadoopSpark/./lib2/test2.jar"));
  }

    @Test
    public void testTwoLibFolderExpansionExpandsInOrder() throws IOException {

      executionJarFile.delete();

      File lib2FolderFile = new File(workingDirFile, "lib2");
      lib2FolderFile.mkdirs();
      File lib2test1Jar = new File(lib2FolderFile, "test1.jar");
      lib2test1Jar.createNewFile();

      String retval = HadoopJobUtils.resolveWildCardForJarSpec(workingDirString, "./lib/*,./lib2/*",
              logger);

      Assert.assertEquals(
              retval,
              "/tmp/TestHadoopSpark/./lib/library.jar,/tmp/TestHadoopSpark/./lib2/test1.jar");
  }
}
