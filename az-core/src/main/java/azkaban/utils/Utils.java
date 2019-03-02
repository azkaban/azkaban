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
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.Random;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.Seconds;
import org.joda.time.Weeks;
import org.joda.time.Years;
import org.quartz.CronExpression;

/**
 * A util helper class full of static methods that are commonly used.
 */
public class Utils {

  public static final Random RANDOM = new Random();
  private static final Logger logger = Logger
      .getLogger(Utils.class);

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
              path + input.getName() + (f.isDirectory() ? "/" : "");
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
   * Get the Class of all the objects
   *
   * @param args The objects to get the Classes from
   * @return The classes as an array
   */
  public static Class<?>[] getTypes(final Object... args) {
    final Class<?>[] argTypes = new Class<?>[args.length];
    for (int i = 0; i < argTypes.length; i++) {
      argTypes[i] = args[i].getClass();
    }
    return argTypes;
  }

  public static Object callConstructor(final Class<?> c, final Object... args) {
    return callConstructor(c, getTypes(args), args);
  }

  /**
   * Call the class constructor with the given arguments
   *
   * @param c The class
   * @param args The arguments
   * @return The constructed object
   */
  public static Object callConstructor(final Class<?> c, final Class<?>[] argTypes,
      final Object[] args) {
    try {
      final Constructor<?> cons = c.getConstructor(argTypes);
      return cons.newInstance(args);
    } catch (final InvocationTargetException e) {
      throw getCause(e);
    } catch (final IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (final NoSuchMethodException e) {
      throw new IllegalStateException(e);
    } catch (final InstantiationException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String formatDuration(final long startTime, final long endTime) {
    if (startTime == -1) {
      return "-";
    }

    final long durationMS;
    if (endTime == -1) {
      durationMS = DateTime.now().getMillis() - startTime;
    } else {
      durationMS = endTime - startTime;
    }

    long seconds = durationMS / 1000;
    if (seconds < 60) {
      return seconds + " sec";
    }

    long minutes = seconds / 60;
    seconds %= 60;
    if (minutes < 60) {
      return minutes + "m " + seconds + "s";
    }

    long hours = minutes / 60;
    minutes %= 60;
    if (hours < 24) {
      return hours + "h " + minutes + "m " + seconds + "s";
    }

    final long days = hours / 24;
    hours %= 24;
    return days + "d " + hours + "h " + minutes + "m";
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

  public static ReadablePeriod parsePeriodString(final String periodStr) {
    final ReadablePeriod period;
    final char periodUnit = periodStr.charAt(periodStr.length() - 1);
    if (periodStr.equals("null") || periodUnit == 'n') {
      return null;
    }

    final int periodInt =
        Integer.parseInt(periodStr.substring(0, periodStr.length() - 1));
    switch (periodUnit) {
      case 'y':
        period = Years.years(periodInt);
        break;
      case 'M':
        period = Months.months(periodInt);
        break;
      case 'w':
        period = Weeks.weeks(periodInt);
        break;
      case 'd':
        period = Days.days(periodInt);
        break;
      case 'h':
        period = Hours.hours(periodInt);
        break;
      case 'm':
        period = Minutes.minutes(periodInt);
        break;
      case 's':
        period = Seconds.seconds(periodInt);
        break;
      default:
        throw new IllegalArgumentException("Invalid schedule period unit '"
            + periodUnit);
    }

    return period;
  }

  public static String createPeriodString(final ReadablePeriod period) {
    String periodStr = "null";

    if (period == null) {
      return "null";
    }

    if (period.get(DurationFieldType.years()) > 0) {
      final int years = period.get(DurationFieldType.years());
      periodStr = years + "y";
    } else if (period.get(DurationFieldType.months()) > 0) {
      final int months = period.get(DurationFieldType.months());
      periodStr = months + "M";
    } else if (period.get(DurationFieldType.weeks()) > 0) {
      final int weeks = period.get(DurationFieldType.weeks());
      periodStr = weeks + "w";
    } else if (period.get(DurationFieldType.days()) > 0) {
      final int days = period.get(DurationFieldType.days());
      periodStr = days + "d";
    } else if (period.get(DurationFieldType.hours()) > 0) {
      final int hours = period.get(DurationFieldType.hours());
      periodStr = hours + "h";
    } else if (period.get(DurationFieldType.minutes()) > 0) {
      final int minutes = period.get(DurationFieldType.minutes());
      periodStr = minutes + "m";
    } else if (period.get(DurationFieldType.seconds()) > 0) {
      final int seconds = period.get(DurationFieldType.seconds());
      periodStr = seconds + "s";
    }

    return periodStr;
  }

  /**
   * @param strMemSize : memory string in the format such as 1G, 500M, 3000K, 5000
   * @return : long value of memory amount in kb
   */
  public static long parseMemString(final String strMemSize) {
    if (strMemSize == null) {
      return 0L;
    }

    long size = 0L;
    if (strMemSize.endsWith("g") || strMemSize.endsWith("G")
        || strMemSize.endsWith("m") || strMemSize.endsWith("M")
        || strMemSize.endsWith("k") || strMemSize.endsWith("K")) {
      final String strSize = strMemSize.substring(0, strMemSize.length() - 1);
      size = Long.parseLong(strSize);
    } else {
      size = Long.parseLong(strMemSize);
    }

    long sizeInKb = 0L;
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
    if (cronExecutionTime == null || cronExecutionTime.getNextValidTimeAfter(new Date()) == null) {
      return false;
    }
    return true;
  }

}
