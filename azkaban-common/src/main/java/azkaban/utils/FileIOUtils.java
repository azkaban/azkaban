/*
 * Copyright 2012 LinkedIn Corp.
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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;

/**
 * Runs a few unix commands. Created this so that I can move to JNI in the
 * future.
 */
public class FileIOUtils {

  public static class PrefixSuffixFileFilter implements FileFilter {
    private String prefix;
    private String suffix;

    public PrefixSuffixFileFilter(String prefix, String suffix) {
      this.prefix = prefix;
      this.suffix = suffix;
    }

    @Override
    public boolean accept(File pathname) {
      if (!pathname.isFile() || pathname.isHidden()) {
        return false;
      }

      String name = pathname.getName();
      int length = name.length();
      if (suffix.length() > length || prefix.length() > length) {
        return false;
      }

      return name.startsWith(prefix) && name.endsWith(suffix);
    }
  }

  public static String getSourcePathFromClass(Class<?> containedClass) {
    File file =
        new File(containedClass.getProtectionDomain().getCodeSource()
            .getLocation().getPath());

    if (!file.isDirectory() && file.getName().endsWith(".class")) {
      String name = containedClass.getName();
      StringTokenizer tokenizer = new StringTokenizer(name, ".");
      while (tokenizer.hasMoreTokens()) {
        tokenizer.nextElement();
        file = file.getParentFile();
      }
      return file.getPath();
    } else {
      return containedClass.getProtectionDomain().getCodeSource().getLocation()
          .getPath();
    }
  }

  /**
   * Run a unix command that will symlink files, and recurse into directories.
   */
  public static void createDeepSymlink(File sourceDir, File destDir)
      throws IOException {
    if (!sourceDir.exists()) {
      throw new IOException("Source directory " + sourceDir.getPath()
          + " doesn't exist");
    } else if (!destDir.exists()) {
      throw new IOException("Destination directory " + destDir.getPath()
          + " doesn't exist");
    } else if (sourceDir.isFile() && destDir.isFile()) {
      throw new IOException("Source or Destination is not a directory.");
    }

    Set<String> paths = new HashSet<String>();
    createDirsFindFiles(sourceDir, sourceDir, destDir, paths);

    StringBuffer buffer = new StringBuffer();
    for (String path : paths) {
      File sourceLink = new File(sourceDir, path);
      path = "." + path;

      buffer.append("ln -s ").append(sourceLink.getAbsolutePath()).append("/*")
          .append(" ").append(path).append(";");
    }

    String command = buffer.toString();
    ProcessBuilder builder = new ProcessBuilder().command("sh", "-c", command);
    builder.directory(destDir);

    // XXX what about stopping threads ??
    Process process = builder.start();
    try {
      NullLogger errorLogger = new NullLogger(process.getErrorStream());
      NullLogger inputLogger = new NullLogger(process.getInputStream());
      errorLogger.start();
      inputLogger.start();

      try {
        if (process.waitFor() < 0) {
          // Assume that the error will be in standard out. Otherwise it'll be
          // in standard in.
          String errorMessage = errorLogger.getLastMessages();
          if (errorMessage.isEmpty()) {
            errorMessage = inputLogger.getLastMessages();
          }

          throw new IOException(errorMessage);
        }

        // System.out.println(errorLogger.getLastMessages());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } finally {
      IOUtils.closeQuietly(process.getInputStream());
      IOUtils.closeQuietly(process.getOutputStream());
      IOUtils.closeQuietly(process.getErrorStream());
    }
  }

  private static void createDirsFindFiles(File baseDir, File sourceDir,
      File destDir, Set<String> paths) {
    File[] srcList = sourceDir.listFiles();
    String path = getRelativePath(baseDir, sourceDir);
    paths.add(path);

    for (File file : srcList) {
      if (file.isDirectory()) {
        File newDestDir = new File(destDir, file.getName());
        newDestDir.mkdirs();
        createDirsFindFiles(baseDir, file, newDestDir, paths);
      }
    }
  }

  private static String getRelativePath(File basePath, File sourceDir) {
    return sourceDir.getPath().substring(basePath.getPath().length());
  }

  private static class NullLogger extends Thread {
    private final BufferedReader inputReader;
    private CircularBuffer<String> buffer = new CircularBuffer<String>(5);

    public NullLogger(InputStream stream) {
      inputReader = new BufferedReader(new InputStreamReader(stream));
    }

    public void run() {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          String line = inputReader.readLine();
          if (line == null) {
            return;
          }
          buffer.append(line);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public String getLastMessages() {
      StringBuffer messageBuffer = new StringBuffer();
      for (String message : buffer) {
        messageBuffer.append(message);
        messageBuffer.append("\n");
      }

      return messageBuffer.toString();
    }
  }

  public static Pair<Integer, Integer> readUtf8File(File file, int offset,
      int length, OutputStream stream) throws IOException {
    byte[] buffer = new byte[length];

    FileInputStream fileStream = new FileInputStream(file);

    long skipped = fileStream.skip(offset);
    if (skipped < offset) {
      fileStream.close();
      return new Pair<Integer, Integer>(0, 0);
    }

    BufferedInputStream inputStream = null;
    try {
      inputStream = new BufferedInputStream(fileStream);
      inputStream.read(buffer);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }

    Pair<Integer, Integer> utf8Range = getUtf8Range(buffer, 0, length);
    stream.write(buffer, utf8Range.getFirst(), utf8Range.getSecond());

    return new Pair<Integer, Integer>(offset + utf8Range.getFirst(),
        utf8Range.getSecond());
  }

  public static LogData readUtf8File(File file, int fileOffset, int length)
      throws IOException {
    byte[] buffer = new byte[length];
    FileInputStream fileStream = new FileInputStream(file);

    long skipped = fileStream.skip(fileOffset);
    if (skipped < fileOffset) {
      fileStream.close();
      return new LogData(fileOffset, 0, "");
    }

    BufferedInputStream inputStream = null;
    int read = 0;
    try {
      inputStream = new BufferedInputStream(fileStream);
      read = inputStream.read(buffer);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }

    if (read <= 0) {
      return new LogData(fileOffset, 0, "");
    }
    Pair<Integer, Integer> utf8Range = getUtf8Range(buffer, 0, read);
    String outputString =
        new String(buffer, utf8Range.getFirst(), utf8Range.getSecond());

    return new LogData(fileOffset + utf8Range.getFirst(),
        utf8Range.getSecond(), outputString);
  }

  public static JobMetaData readUtf8MetaDataFile(File file, int fileOffset,
      int length) throws IOException {
    byte[] buffer = new byte[length];
    FileInputStream fileStream = new FileInputStream(file);

    long skipped = fileStream.skip(fileOffset);
    if (skipped < fileOffset) {
      fileStream.close();
      return new JobMetaData(fileOffset, 0, "");
    }

    BufferedInputStream inputStream = null;
    int read = 0;
    try {
      inputStream = new BufferedInputStream(fileStream);
      read = inputStream.read(buffer);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }

    if (read <= 0) {
      return new JobMetaData(fileOffset, 0, "");
    }
    Pair<Integer, Integer> utf8Range = getUtf8Range(buffer, 0, read);
    String outputString =
        new String(buffer, utf8Range.getFirst(), utf8Range.getSecond());

    return new JobMetaData(fileOffset + utf8Range.getFirst(),
        utf8Range.getSecond(), outputString);
  }

  /**
   * Returns first and length.
   */
  public static Pair<Integer, Integer> getUtf8Range(byte[] buffer, int offset,
      int length) {
    int start = getUtf8ByteStart(buffer, offset);
    int end = getUtf8ByteEnd(buffer, offset + length - 1);

    return new Pair<Integer, Integer>(start, end - start + 1);
  }

  private static int getUtf8ByteStart(byte[] buffer, int offset) {
    // If it's a proper utf-8, we should find it within the next 6 bytes.
    for (int i = offset; i < offset + 6 && i < buffer.length; i++) {
      byte b = buffer[i];
      // check the mask 0x80 is 0, which is a proper ascii
      if ((0x80 & b) == 0) {
        return i;
      } else if ((0xC0 & b) == 0xC0) {
        return i;
      }
    }

    // Don't know what it is, will just set it as 0
    return offset;
  }

  private static int getUtf8ByteEnd(byte[] buffer, int offset) {
    // If it's a proper utf-8, we should find it within the previous 12 bytes.
    for (int i = offset; i > offset - 11 && i >= 0; i--) {
      byte b = buffer[i];
      // check the mask 0x80 is 0, which is a proper ascii. Just return
      if ((0x80 & b) == 0) {
        return i;
      }

      if ((b & 0xE0) == 0xC0) { // two byte utf8 char. bits 110x xxxx
        if (offset - i >= 1) {
          // There is 1 following byte we're good.
          return i + 1;
        }
      } else if ((b & 0xF0) == 0xE0) { // three byte utf8 char. bits 1110 xxxx
        if (offset - i >= 2) {
          // There is 1 following byte we're good.
          return i + 2;
        }
      } else if ((b & 0xF8) == 0xF0) { // four byte utf8 char. bits 1111 0xxx
        if (offset - i >= 3) {
          // There is 1 following byte we're good.
          return i + 3;
        }
      } else if ((b & 0xFC) >= 0xF8) { // five byte utf8 char. bits 1111 10xx
        if (offset - i == 4) {
          // There is 1 following byte we're good.
          return i + 4;
        }
      } else if ((b & 0xFE) == 0xFC) { // six byte utf8 char. bits 1111 110x
        if (offset - i >= 5) {
          // There is 1 following byte we're good.
          return i + 5;
        }
      }
    }

    // Don't know what it is, will just set it as 0
    return offset;
  }

  public static class LogData {
    private int offset;
    private int length;
    private String data;

    public LogData(int offset, int length, String data) {
      this.offset = offset;
      this.length = length;
      this.data = data;
    }

    public int getOffset() {
      return offset;
    }

    public int getLength() {
      return length;
    }

    public String getData() {
      return data;
    }

    public Map<String, Object> toObject() {
      HashMap<String, Object> map = new HashMap<String, Object>();
      map.put("offset", offset);
      map.put("length", length);
      map.put("data", data);

      return map;
    }

    public static LogData createLogDataFromObject(Map<String, Object> map) {
      int offset = (Integer) map.get("offset");
      int length = (Integer) map.get("length");
      String data = (String) map.get("data");

      return new LogData(offset, length, data);
    }

    @Override
    public String toString() {
      return "[offset=" + offset + ",length=" + length + ",data=" + data + "]";
    }
  }

  public static class JobMetaData {
    private int offset;
    private int length;
    private String data;

    public JobMetaData(int offset, int length, String data) {
      this.offset = offset;
      this.length = length;
      this.data = data;
    }

    public int getOffset() {
      return offset;
    }

    public int getLength() {
      return length;
    }

    public String getData() {
      return data;
    }

    public Map<String, Object> toObject() {
      HashMap<String, Object> map = new HashMap<String, Object>();
      map.put("offset", offset);
      map.put("length", length);
      map.put("data", data);

      return map;
    }

    public static JobMetaData createJobMetaDataFromObject(
        Map<String, Object> map) {
      int offset = (Integer) map.get("offset");
      int length = (Integer) map.get("length");
      String data = (String) map.get("data");

      return new JobMetaData(offset, length, data);
    }

    @Override
    public String toString() {
      return "[offset=" + offset + ",length=" + length + ",data=" + data + "]";
    }
  }
}
