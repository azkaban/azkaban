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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.quartz.CronExpression;


/**
 * A util helper class full of static methods that are commonly used.
 */
public class Utils {

  private static final Random RANDOM = new Random();
  private static final Logger logger = Logger.getLogger(Utils.class);

  /**
   * Private constructor.
   */
  private Utils() {
  }


  /**
   * Equivalent to Object.equals except that it handles nulls. If a and b are both null, true is
   * returned.
   */
  public static boolean equals(final Object a, final Object b) {
    if (a == null || b == null) {
      return a == b;
    }

    return a.equals(b);
  }

  /**
   * Return the object if it is non-null, otherwise throw an exception
   *
   * @param <T> The type of the object
   * @param t The object
   * @return The object if it is not null
   * @throws IllegalArgumentException if the object is null
   */
  public static <T> T nonNull(final T t) {
    if (t == null) {
      throw new IllegalArgumentException("Null value not allowed.");
    } else {
      return t;
    }
  }

  public static File findFilefromDir(final File dir, final String fn) {
    if (dir.isDirectory()) {
      for (final File f : dir.listFiles()) {
        if (f.getName().equals(fn)) {
          return f;
        }
      }
    }
    return null;
  }

  /**
   * Return the value itself if it is non-null, otherwise return the default value
   *
   * @param value The object
   * @param defaultValue default value if object == null
   * @param <T> The type of the object
   * @return The object itself or default value when it is null
   */
  public static <T> T ifNull(final T value, final T defaultValue) {
    return (value == null) ? defaultValue : value;
  }

  /**
   * Print the message and then exit with the given exit code
   *
   * @param message The message to print
   * @param exitCode The exit code
   */
  public static void croak(final String message, final int exitCode) {
    System.err.println(message);
    System.exit(exitCode);
  }

  /**
   * Tests whether a port is valid or not
   *
   * @return true, if port is valid
   */
  public static boolean isValidPort(final int port) {
    if (port >= 1 && port <= 65535) {
      return true;
    }
    return false;
  }

  public static File createTempDir() {
    return createTempDir(new File(System.getProperty("java.io.tmpdir")));
  }

  public static File createTempDir(final File parent) {
    final File temp =
        new File(parent,
            Integer.toString(Math.abs(RANDOM.nextInt()) % 100000000));
    temp.delete();
    temp.mkdir();
    temp.deleteOnExit();
    return temp;
  }

  public static void zip(final File input, final File output) throws IOException {
    final FileOutputStream out = new FileOutputStream(output);
    final ZipOutputStream zOut = new ZipOutputStream(out);
    try {
      zipFile("", input, zOut);
    } finally {
      zOut.close();
    }
  }

  public static void zipFolderContent(final File folder, final File output)
      throws IOException {
    final FileOutputStream out = new FileOutputStream(output);
    final ZipOutputStream zOut = new ZipOutputStream(out);
    try {
      final File[] files = folder.listFiles();
      if (files != null) {
        for (final File f : files) {
          zipFile("", f, zOut);
        }
      }
    } finally {
      zOut.close();
    }
  }

  private static void zipFile(final String path, final File input, final ZipOutputStream zOut)
      throws IOException {
    if (input.isDirectory()) {
      final File[] files = input.listFiles();
      if (files != null) {
        for (final File f : files) {
          final String childPath =
              path + input.getName() + (f.isDirectory() ? File.separator : "");
          zipFile(childPath, f, zOut);
        }
      }
    } else {
      final String childPath =
          path + (path.length() > 0 ? "/" : "") + input.getName();
      final ZipEntry entry = new ZipEntry(childPath);
      zOut.putNextEntry(entry);
      final InputStream fileInputStream =
          new BufferedInputStream(new FileInputStream(input));
      try {
        IOUtils.copy(fileInputStream, zOut);
      } finally {
        fileInputStream.close();
      }
    }
  }

  public static void unzip(final ZipFile source, final File dest) throws IOException {
    final Enumeration<?> entries = source.entries();
    while (entries.hasMoreElements()) {
      final ZipEntry entry = (ZipEntry) entries.nextElement();
      final File newFile = new File(dest, entry.getName());
      if (!newFile.getCanonicalPath().startsWith(dest.getCanonicalPath())) {
        throw new IOException(
            "Extracting zip entry would have resulted in a file outside the specified destination"
                + " directory.");
      }

      if (entry.isDirectory()) {
        newFile.mkdirs();
      } else {
        newFile.getParentFile().mkdirs();
        final InputStream src = source.getInputStream(entry);
        try {
          final OutputStream output =
              new BufferedOutputStream(new FileOutputStream(newFile));
          try {
            IOUtils.copy(src, output);
          } finally {
            output.close();
          }
        } finally {
          src.close();
        }
      }
    }
  }

  public static String flattenToString(final Collection<?> collection,
      final String delimiter) {
    final StringBuffer buffer = new StringBuffer();
    for (final Object obj : collection) {
      buffer.append(obj.toString());
      buffer.append(delimiter);
    }

    if (buffer.length() > 0) {
      buffer.setLength(buffer.length() - 1);
    }
    return buffer.toString();
  }

  public static Double convertToDouble(final Object obj) {
    if (obj instanceof String) {
      return Double.parseDouble((String) obj);
    }
    return (Double) obj;
  }

  /**
   * Get the root cause of the Exception
   *
   * @param e The Exception
   * @return The root cause of the Exception
   */
  private static RuntimeException getCause(final InvocationTargetException e) {
    final Throwable cause = e.getCause();
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    } else {
      throw new IllegalStateException(e.getCause());
    }
  }

  /**
   * Construct a class object with the given arguments
   *
   * @param cls The class
   * @param args The arguments
   * @return Constructed Object
   */
  public static Object callConstructor(final Class<?> cls, final Object... args) {
    return callConstructor(cls, getTypes(args), args);
  }

  /**
   * Get the Class of all the objects
   *
   * @param args The objects to get the Classes from
   * @return The classes as an array
   */
  private static Class<?>[] getTypes(final Object... args) {
    final Class<?>[] argTypes = new Class<?>[args.length];
    for (int i = 0; i < argTypes.length; i++) {
      argTypes[i] = args[i].getClass();
    }
    return argTypes;
  }

  /**
   * Call the class constructor with the given arguments
   *
   * @param cls The class
   * @param args The arguments
   * @return The constructed object
   */
  private static Object callConstructor(final Class<?> cls, final Class<?>[] argTypes,
      final Object[] args) {
    try {
      final Constructor<?> cons = cls.getConstructor(argTypes);
      return cons.newInstance(args);
    } catch (final InvocationTargetException e) {
      throw getCause(e);
    } catch (final IllegalAccessException | NoSuchMethodException | InstantiationException e) {
      throw new IllegalStateException(e);
    }
  }

  public static Object invokeStaticMethod(final ClassLoader loader, final String className,
      final String methodName, final Object... args) throws ClassNotFoundException,
      SecurityException, NoSuchMethodException, IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    final Class<?> clazz = loader.loadClass(className);

    final Class<?>[] argTypes = new Class[args.length];
    for (int i = 0; i < args.length; ++i) {
      // argTypes[i] = args[i].getClass();
      argTypes[i] = args[i].getClass();
    }

    final Method method = clazz.getDeclaredMethod(methodName, argTypes);
    return method.invoke(null, args);
  }

  public static void copyStream(final InputStream input, final OutputStream output)
      throws IOException {
    final byte[] buffer = new byte[1024];
    int bytesRead;
    while ((bytesRead = input.read(buffer)) != -1) {
      output.write(buffer, 0, bytesRead);
    }
  }

  /**
   * @param strMemSize : memory string in the format such as 1G, 500M, 3000K, 5000
   * @return : long value of memory amount in kb
   */
  public static long parseMemString(final String strMemSize) {
    if (strMemSize == null) {
      return 0L;
    }

    final long size;
    if (strMemSize.endsWith("g") || strMemSize.endsWith("G")
        || strMemSize.endsWith("m") || strMemSize.endsWith("M")
        || strMemSize.endsWith("k") || strMemSize.endsWith("K")) {
      final String strSize = strMemSize.substring(0, strMemSize.length() - 1);
      size = Long.parseLong(strSize);
    } else {
      size = Long.parseLong(strMemSize);
    }

    final long sizeInKb;
    if (strMemSize.endsWith("g") || strMemSize.endsWith("G")) {
      sizeInKb = size * 1024L * 1024L;
    } else if (strMemSize.endsWith("m") || strMemSize.endsWith("M")) {
      sizeInKb = size * 1024L;
    } else if (strMemSize.endsWith("k") || strMemSize.endsWith("K")) {
      sizeInKb = size;
    } else {
      sizeInKb = size / 1024L;
    }

    return sizeInKb;
  }

  /**
   * @param cronExpression: A cron expression is a string separated by white space, to provide a
   * parser and evaluator for Quartz cron expressions.
   * @return : org.quartz.CronExpression object.
   *
   * TODO: Currently, we have to transform Joda Timezone to Java Timezone due to CronExpression.
   * Since Java8 enhanced Time functionalities, We consider transform all Jodatime to Java Time in
   * future.
   */
  public static CronExpression parseCronExpression(final String cronExpression,
      final DateTimeZone timezone) {
    if (cronExpression != null) {
      try {
        final CronExpression ce = new CronExpression(cronExpression);
        ce.setTimeZone(TimeZone.getTimeZone(timezone.getID()));
        return ce;
      } catch (final ParseException pe) {
        logger.error("this cron expression {" + cronExpression + "} can not be parsed. "
            + "Please Check Quartz Cron Syntax.");
      }
      return null;
    } else {
      return null;
    }
  }

  /**
   * @return if the cronExpression is valid or not.
   */
  public static boolean isCronExpressionValid(final String cronExpression,
      final DateTimeZone timezone) {
    if (!CronExpression.isValidExpression(cronExpression)) {
      return false;
    }

    /*
     * The below code is aimed at checking some cases that the above code can not identify,
     * e.g. <0 0 3 ? * * 22> OR <0 0 3 ? * 8>. Under these cases, the below code is able to tell.
     */
    final CronExpression cronExecutionTime = parseCronExpression(cronExpression, timezone);
    return (!(cronExecutionTime == null
        || cronExecutionTime.getNextValidTimeAfter(new Date()) == null));
  }

  /**
   * Run a sequence of commands
   *
   * @param commands sequence of commands
   * @return list of output result
   */
  public static ArrayList<String> runProcess(String... commands)
      throws InterruptedException, IOException {
    final java.lang.ProcessBuilder processBuilder = new java.lang.ProcessBuilder(commands);
    final ArrayList<String> output = new ArrayList<>();
    final Process process = processBuilder.start();
    process.waitFor();
    final InputStream inputStream = process.getInputStream();
    try {
      final java.io.BufferedReader reader = new java.io.BufferedReader(
          new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      String line;
      while ((line = reader.readLine()) != null) {
        output.add(line);
      }
    } finally {
      inputStream.close();
    }
    return output;
  }

  /**
   * Merge the absolute paths of source paths into the list of destination paths
   *
   * @param destinationPaths the path list which the source paths will be merged into
   * @param sourcePaths source paths
   * @param rootPath defined root path for source paths when they are not absolute path
   */
  public static void mergeTypeClassPaths(
      List<String> destinationPaths, final List<String> sourcePaths, final String rootPath) {
    if (sourcePaths != null) {
      for (String jar : sourcePaths) {
        File file = new File(jar);
        if (!file.isAbsolute()) {
          file = new File(rootPath + File.separatorChar + jar);
        }

        String path = file.getAbsolutePath();
        if (!destinationPaths.contains(path)) {
          destinationPaths.add(path);
        }
      }
    }
  }

  /**
   * Merge elements in Source List into the Destination List
   *
   * @param destinationList the list which the source elements will be merged into
   * @param sourceList source List
   */
  public static void mergeStringList(
      final List<String> destinationList, final List<String> sourceList) {
    if (sourceList != null) {
      for (String item : sourceList) {
        if (!destinationList.contains(item)) {
          destinationList.add(item);
        }
      }
    }
  }
}
