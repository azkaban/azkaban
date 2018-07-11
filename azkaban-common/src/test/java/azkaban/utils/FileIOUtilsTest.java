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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.NameFileComparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileIOUtilsTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File sourceDir, destDir, baseDir;

  /**
   * Create a very big dir which would cause linux shell command to hard link the dir to
   * exceed the allowed limit.
   */
  private void createBigDir(final String path) throws IOException {
    final String verylongprefix =
        "123123123123123123123123123123123113123111111111111111111111111111111111111111111111111111"
            + "111111111111111111111111111111111111111111111111111111111111111111111111111111111111"
            + "11231312312312312312313121111111111111111111111111111111111111111111111111";

    for (int i = 1; i <= 400; i++) {
      final Path tmpDirPath = Paths.get(path, verylongprefix + "dir" + i);
      Files.createDirectory(tmpDirPath);
      for (int j = 1; j <= 100; j++) {
        final Path tmp = Paths.get(tmpDirPath.toAbsolutePath().toString(), String.valueOf(j));
        Files.createFile(tmp);
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    // setup base dir

    this.baseDir = this.temp.newFolder("base");
    final File file1 = new File(this.baseDir.getAbsolutePath() + "/a.out");
    final File file2 = new File(this.baseDir.getAbsolutePath() + "/testdir");
    final File file3 = new File(file2.getAbsolutePath() + "/b.out");
    final File file4 = new File(file2.getAbsolutePath() + "/testdir");
    final File file5 = new File(file2.getAbsolutePath() + "/c.out");
    final File file6 = new File(file4.getAbsolutePath() + "/c1.out");
    final File file7 = new File(file4.getAbsolutePath() + "/c2.out");

    file1.createNewFile();
    file2.mkdir();
    file3.createNewFile();
    file4.mkdir();
    file5.createNewFile();
    file6.createNewFile();
    file7.createNewFile();

    byte[] fileData = new byte[]{1, 2, 3};
    FileOutputStream out = new FileOutputStream(file1);
    out.write(fileData);
    out.close();

    fileData = new byte[]{2, 3, 4};
    out = new FileOutputStream(file3);
    out.write(fileData);
    out.close();

    this.sourceDir = this.temp.newFolder("src");
    FileUtils.copyDirectory(this.baseDir, this.sourceDir);

    // setup target dir
    this.destDir = this.temp.newFolder("dest");
  }

  @After
  public void tearDown() throws Exception {
    this.temp.delete();
    FileUtils.deleteDirectory(this.baseDir);
    FileUtils.deleteDirectory(this.sourceDir);
    FileUtils.deleteDirectory(this.destDir);
  }


  private File dumpNumberToTempFile(final String fileName, final long num) throws IOException {
    final File fileToDump = this.temp.newFile(fileName);
    FileIOUtils.dumpNumberToFile(fileToDump.toPath(), num);
    return fileToDump;
  }

  @Test
  public void testDumpNumberToFileAndReadFromFile() throws IOException {
    final String fileName = "number";
    final long num = 94127;
    final File fileToDump = dumpNumberToTempFile(fileName, num);
    assertThat(FileIOUtils.readNumberFromFile(fileToDump.toPath())).isEqualTo(num);
  }

  @Test
  public void testDumpNumberToExistingFile() throws IOException {
    final String fileName = "number";
    final long firstNum = 94127;
    final long secondNum = 94128;
    dumpNumberToTempFile(fileName, firstNum);
    assertThatThrownBy(() -> dumpNumberToTempFile(fileName, secondNum))
        .isInstanceOf(IOException.class).hasMessageContaining("already exists");
  }

  @Test
  public void testHardlinkCopy() throws IOException {
    FileIOUtils.createDeepHardlink(this.sourceDir, this.destDir);
    assertThat(areDirsEqual(this.sourceDir, this.destDir, true)).isTrue();
    FileUtils.deleteDirectory(this.destDir);
    assertThat(areDirsEqual(this.baseDir, this.sourceDir, true)).isTrue();
  }

  @Ignore("Slow test (over 30s) - run manually if need to touch createDeepHardlink()")
  @Test
  public void testHardlinkCopyOfBigDir() throws IOException {
    final File bigDir = new File(this.baseDir.getAbsolutePath() + "/bigdir");
    bigDir.mkdir();
    createBigDir(bigDir.getAbsolutePath());

    FileIOUtils.createDeepHardlink(bigDir, this.destDir);
    assertThat(areDirsEqual(this.destDir, bigDir, true)).isTrue();
    FileUtils.deleteDirectory(bigDir);

  }

  @Test
  public void testHardlinkCopyNonSource() {
    assertThatThrownBy(() -> {
      FileIOUtils.createDeepHardlink(new File(this.sourceDir, "idonotexist"), this.destDir);
    }).isInstanceOf(IOException.class);
  }

  private boolean areDirsEqualUtil(final File file1, final File file2, final boolean isRoot,
      final boolean ignoreRoot)
      throws IOException {
    if (!file1.getName().equals(file2.getName())) {
      if (!isRoot && ignoreRoot) {
        return false;
      }
    }
    if (file1.isDirectory() && file2.isDirectory()) {
      if (file1.listFiles().length != file2.listFiles().length) {
        return false;
      }
      final File[] fileList1 = file1.listFiles();
      final File[] fileList2 = file2.listFiles();
      Arrays.sort(fileList1, NameFileComparator.NAME_COMPARATOR);
      Arrays.sort(fileList2, NameFileComparator.NAME_COMPARATOR);

      for (int i = 0; i < fileList1.length; i++) {
        if (!areDirsEqualUtil(fileList1[i], fileList2[i], false, ignoreRoot)) {
          return false;
        }
      }
      return true;
    } else if (file1.isFile() && file2.isFile()) {
      return file1.getName().equals(file2.getName()) && FileUtils.contentEquals(file1, file2);
    } else {
      return false;
    }
  }


  // check if two dirs are structurally same and contains files of same content
  private boolean areDirsEqual(final File file1, final File file2, final boolean ignoreRoot)
      throws IOException {
    return areDirsEqualUtil(file1, file2, true, ignoreRoot);
  }

  @Test
  public void testAsciiUTF() throws IOException {
    final String foreignText = "abcdefghijklmnopqrstuvwxyz";
    final byte[] utf8ByteArray = createUTF8ByteArray(foreignText);

    final int length = utf8ByteArray.length;
    System.out.println("char length:" + foreignText.length() +
        " utf8BytesLength:" + utf8ByteArray.length + " for:" + foreignText);

    final Pair<Integer, Integer> pair = FileIOUtils.getUtf8Range(utf8ByteArray, 1,
        length - 6);
    System.out.println("Pair :" + pair.toString());

    final String recreatedString = new String(utf8ByteArray, 1, length - 6, "UTF-8");
    System.out.println("recreatedString:" + recreatedString);

    final String correctString = new String(utf8ByteArray, pair.getFirst(),
        pair.getSecond(), "UTF-8");
    System.out.println("correctString:" + correctString);

    assertEquals(pair, new Pair<>(1, 20));
    // Two characters stripped from this.
    assertEquals(correctString.length(), foreignText.length() - 6);

  }

  @Test
  public void testForeignUTF() throws IOException {
    final String foreignText = "안녕하세요, 제 이름은 박병호입니다";
    final byte[] utf8ByteArray = createUTF8ByteArray(foreignText);

    final int length = utf8ByteArray.length;
    System.out.println("char length:" + foreignText.length()
        + " utf8BytesLength:" + utf8ByteArray.length + " for:" + foreignText);

    final Pair<Integer, Integer> pair = FileIOUtils.getUtf8Range(utf8ByteArray, 1,
        length - 6);
    System.out.println("Pair :" + pair.toString());

    final String recreatedString = new String(utf8ByteArray, 1, length - 6, "UTF-8");
    System.out.println("recreatedString:" + recreatedString);

    String correctString = new String(utf8ByteArray, pair.getFirst(),
        pair.getSecond(), "UTF-8");
    System.out.println("correctString:" + correctString);

    assertEquals(pair, new Pair<>(3, 40));
    // Two characters stripped from this.
    assertEquals(correctString.length(), foreignText.length() - 3);

    // Testing mixed bytes
    final String mixedText = "abc안녕하세요, 제 이름은 박병호입니다";
    final byte[] mixedBytes = createUTF8ByteArray(mixedText);
    final Pair<Integer, Integer> pair2 = FileIOUtils.getUtf8Range(mixedBytes, 1,
        length - 4);
    correctString = new String(mixedBytes, pair2.getFirst(), pair2.getSecond(),
        "UTF-8");
    System.out.println("correctString:" + correctString);
    assertEquals(pair2, new Pair<>(1, 45));
    // Two characters stripped from this.
    assertEquals(correctString.length(), mixedText.length() - 3);
  }

  private byte[] createUTF8ByteArray(final String text) {
    byte[] textBytes = null;
    try {
      textBytes = text.getBytes("UTF-8");
    } catch (final UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return textBytes;
  }
}
