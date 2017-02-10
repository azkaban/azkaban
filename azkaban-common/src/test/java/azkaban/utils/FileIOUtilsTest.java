/*
 * Copyright 2014 LinkedIn Corp.
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.NameFileComparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class FileIOUtilsTest {
  private File sourceDir, destDir, baseDir;
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    // setup base dir
    baseDir = temp.newFolder("base");
    File file1 = new File(baseDir.getAbsolutePath()+"/a.out");
    File file2 = new File(baseDir.getAbsolutePath()+"/testdir");
    File file3 = new File(file2.getAbsolutePath()+"/b.out");
    file1.createNewFile();
    file2.mkdir();
    file3.createNewFile();


    byte[] fileData = new byte[]{1,2,3};
    FileOutputStream out = new FileOutputStream(file1);
    out.write(fileData);
    out.close();

    fileData = new byte[]{2,3,4};
    out = new FileOutputStream(file3);
    out.write(fileData);
    out.close();

    sourceDir = temp.newFolder("src");
    FileUtils.copyDirectory(baseDir, sourceDir);

    // setup target dir
    destDir = temp.newFolder("dest");
  }

  @After
  public void tearDown() throws Exception {
    temp.delete();
    FileUtils.deleteDirectory(baseDir);
    FileUtils.deleteDirectory(sourceDir);
    FileUtils.deleteDirectory(destDir);
  }

  @Test
  public void testHardlinkCopy() throws IOException {
    FileIOUtils.createDeepHardlink(sourceDir, destDir);
    assertTrue(areDirsEqual(sourceDir, destDir, true));
    FileUtils.deleteDirectory(destDir);
    assertTrue(areDirsEqual(baseDir, sourceDir, true));
  }

  @Test
  public void testHardlinkCopyNonSource() {
    boolean exception = false;
    try {
      FileIOUtils.createDeepHardlink(new File(sourceDir, "idonotexist"), destDir);
    } catch (IOException e) {
      System.out.println(e.getMessage());
      System.out.println("Handled this case nicely.");
      exception = true;
    }

    assertTrue(exception);
  }

  private boolean areDirsEqualUtil(File file1, File file2, boolean isRoot, boolean ignoreRoot) throws IOException {
    if(!file1.getName().equals(file2.getName())) {
      if(!isRoot && ignoreRoot) return false;
    }
    if(file1.isDirectory() && file2.isDirectory()) {
      if(file1.listFiles().length != file2.listFiles().length) {
        return false;
      }
      File[] fileList1 = file1.listFiles(), fileList2 = file2.listFiles();
      Arrays.sort(fileList1, NameFileComparator.NAME_COMPARATOR);
      Arrays.sort(fileList2, NameFileComparator.NAME_COMPARATOR);

      for(int i = 0; i < fileList1.length; i++) {
        if(!areDirsEqualUtil(fileList1[i], fileList2[i], false, ignoreRoot)) {
          return false;
        }
      }
      return true;
    }
    else if(file1.isFile() && file2.isFile()) {
      return file1.getName().equals(file2.getName()) && FileUtils.contentEquals(file1, file2);
    }
    else return false;
  }


  // check if two dirs are structurally same and contains files of same content
  private boolean areDirsEqual(File file1, File file2, boolean ignoreRoot) throws IOException {
    return areDirsEqualUtil(file1, file2, true, ignoreRoot);
  }

  @Test
  public void testAsciiUTF() throws IOException {
    String foreignText = "abcdefghijklmnopqrstuvwxyz";
    byte[] utf8ByteArray = createUTF8ByteArray(foreignText);

    int length = utf8ByteArray.length;
    System.out.println("char length:" + foreignText.length() +
        " utf8BytesLength:" + utf8ByteArray.length + " for:" + foreignText);

    Pair<Integer,Integer> pair = FileIOUtils.getUtf8Range(utf8ByteArray, 1,
        length - 6);
    System.out.println("Pair :" + pair.toString());

    String recreatedString = new String(utf8ByteArray, 1, length - 6, "UTF-8");
    System.out.println("recreatedString:" + recreatedString);

    String correctString = new String(utf8ByteArray, pair.getFirst(),
        pair.getSecond(), "UTF-8");
    System.out.println("correctString:" + correctString);

    assertEquals(pair, new Pair<Integer,Integer>(1, 20));
    // Two characters stripped from this.
    assertEquals(correctString.length(), foreignText.length() - 6);

  }

  @Test
  public void testForeignUTF() throws IOException {
    String foreignText = "안녕하세요, 제 이름은 박병호입니다";
    byte[] utf8ByteArray = createUTF8ByteArray(foreignText);

    int length = utf8ByteArray.length;
    System.out.println("char length:" + foreignText.length()
        + " utf8BytesLength:" + utf8ByteArray.length + " for:" + foreignText);

    Pair<Integer,Integer> pair = FileIOUtils.getUtf8Range(utf8ByteArray, 1,
        length - 6);
    System.out.println("Pair :" + pair.toString());

    String recreatedString = new String(utf8ByteArray, 1, length - 6, "UTF-8");
    System.out.println("recreatedString:" + recreatedString);

    String correctString = new String(utf8ByteArray, pair.getFirst(),
        pair.getSecond(), "UTF-8");
    System.out.println("correctString:" + correctString);

    assertEquals(pair, new Pair<Integer,Integer>(3, 40));
    // Two characters stripped from this.
    assertEquals(correctString.length(), foreignText.length() - 3);


    // Testing mixed bytes
    String mixedText = "abc안녕하세요, 제 이름은 박병호입니다";
    byte[] mixedBytes = createUTF8ByteArray(mixedText);
    Pair<Integer,Integer> pair2 = FileIOUtils.getUtf8Range(mixedBytes, 1,
        length - 4);
    correctString = new String(mixedBytes, pair2.getFirst(), pair2.getSecond(),
        "UTF-8");
    System.out.println("correctString:" + correctString);
    assertEquals(pair2, new Pair<Integer,Integer>(1, 45));
    // Two characters stripped from this.
    assertEquals(correctString.length(), mixedText.length() - 3);
  }

  private byte[] createUTF8ByteArray(String text) {
    byte[] textBytes= null;
    try {
      textBytes = text.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return textBytes;
  }
}
