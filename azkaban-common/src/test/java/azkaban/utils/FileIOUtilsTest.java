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

import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

public class FileIOUtilsTest {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static File sourceDir;

  private File destDir;

  @BeforeClass
  public static void createSourceDir() throws Exception {
    URL resourceUrl = Resources.getResource("project/testjob");
    assertNotNull(resourceUrl);
    sourceDir = new File(resourceUrl.toURI());
  }

  @Before
  public void setUp() throws Exception {
    destDir = temp.newFolder("unixsymlink");
  }

  @After
  public void tearDown() throws Exception {
    temp.delete();
  }

  @Test
  public void testSymlinkCopy() throws IOException {
    FileIOUtils.createDeepSymlink(sourceDir, destDir);
  }

  @Test
  public void testSymlinkCopyNonSource() {
    boolean exception = false;
    try {
      FileIOUtils.createDeepSymlink(new File(sourceDir, "idonotexist"), destDir);
    } catch (IOException e) {
      System.out.println(e.getMessage());
      System.out.println("Handled this case nicely.");
      exception = true;
    }

    assertTrue(exception);
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
