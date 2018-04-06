package azkaban.jobtype;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import azkaban.jobtype.javautils.FileUtils;

import com.google.common.io.Files;

@SuppressWarnings("DefaultCharset")
public class TestFileUtils {
  private static final int DIRECTORY_LEVEL = 5;
  private static final int NUM_FILES = 5;
  private static final String DELIMITER = ",";

  @Test
  public void testDirectoryDelete() throws IOException {
    File root = Files.createTempDir();
    File parent = root;
    File child = null;
    for (int i=0; i < DIRECTORY_LEVEL; i++) {
      child = createTmpDirWithRandomFiles(parent);
      parent = child;
    }

    Assert.assertTrue("Failed to create " + root.getAbsolutePath(), root.exists());
    FileUtils.deleteFileOrDirectory(root);
    Assert.assertTrue("Failed to delete " + root.getAbsolutePath(), !root.exists());
  }

  @Test
  public void testFileDelete() throws IOException {
    File f = File.createTempFile(this.getClass().getSimpleName(), ".txt");
    BufferedWriter w = new BufferedWriter(new FileWriter(f));
    w.write(RandomStringUtils.randomAlphanumeric(1024));
    w.close();

    Assert.assertTrue("Failed to create " + f.getAbsolutePath(), f.exists());
    FileUtils.deleteFileOrDirectory(f);
    Assert.assertTrue("Failed to delete " + f.getAbsolutePath(), !f.exists());
  }

  private File createTmpDirWithRandomFiles(File parentDir) throws IOException {
    File dir = Files.createTempDir();
    for (int i = 0; i < NUM_FILES; i++) {
      File f = new File(dir, ""+i+".txt");
      f.createNewFile();

      BufferedWriter w = new BufferedWriter(new FileWriter(f));
      w.write(RandomStringUtils.randomAlphanumeric(1024));
      w.close();
    }

    File tmp = new File(parentDir, dir.getName());
    Files.move(dir, tmp);
    return tmp;
  }

  @Test
  public void testlistFiles() throws IOException {
    File root = Files.createTempDir();
    root.deleteOnExit();

    File dir = createTmpDirWithRandomFiles(root);

    //List using wild card
    Collection<String> actual = FileUtils.listFiles(dir.getAbsolutePath() + File.separator + "*", DELIMITER);
    Collection<String> expected = new HashSet<String>();
    for (int i = 0; i < NUM_FILES; i++) {
      expected.add(dir.getAbsolutePath() + File.separator + i +".txt");
    }
    Assert.assertEquals("Failed to list all files with wildcard", expected, new HashSet<String>(actual));

    //List using explicit path
    actual = FileUtils.listFiles(dir.getAbsolutePath() + File.separator + "1.txt" + DELIMITER + dir.getAbsolutePath() + File.separator + "2.txt", DELIMITER);
    expected = new HashSet<String>();
    expected.add(dir.getAbsolutePath() + File.separator + "1.txt");
    expected.add(dir.getAbsolutePath() + File.separator + "2.txt");

    Assert.assertEquals("Failed to list all files", expected, new HashSet<String>(actual));

    FileUtils.deleteFileOrDirectory(root);
  }
}
