package azkaban.jobtype;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import azkaban.utils.Props;

// TODO kunkun-tang: This test class needs more refactors.
public class TestHadoopJobUtilsResolveJarSpec {
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
  @Test(expected = IllegalStateException.class)
  public void testJarDoesNotExist() throws IOException {
    HadoopJobUtils.resolveExecutionJarName(workingDirString, "/lib/abc.jar", logger);
  }

  @Test(expected = IllegalStateException.class)
  public void testNoLibFolder() throws IOException {
    FileUtils.deleteDirectory(libFolderFile);
    HadoopJobUtils.resolveExecutionJarName(workingDirString, "/lib/abc.jar", logger);
  }
}
