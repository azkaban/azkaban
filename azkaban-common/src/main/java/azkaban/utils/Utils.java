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
import java.util.Collection;
import java.util.Enumeration;
import java.util.Random;
import java.util.TimeZone;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;
import java.text.ParseException;

import org.apache.commons.io.IOUtils;

import org.apache.log4j.Logger;

import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;
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

  private static Logger logger = Logger
      .getLogger(Utils.class);
  public static final Random RANDOM = new Random();

  /**
   * Private constructor.
   */
  private Utils() {
  }

  /**
   * Equivalent to Object.equals except that it handles nulls. If a and b are
   * both null, true is returned.
   *
   * @param a
   * @param b
   * @return
   */
  public static boolean equals(Object a, Object b) {
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
  public static <T> T nonNull(T t) {
    if (t == null) {
      throw new IllegalArgumentException("Null value not allowed.");
    } else {
      return t;
    }
  }

  public static File findFilefromDir(File dir, String fn) {
    if (dir.isDirectory()) {
      for (File f : dir.listFiles()) {
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
  public static void croak(String message, int exitCode) {
    System.err.println(message);
    System.exit(exitCode);
  }

  /**
   * Tests whether a port is valid or not
   *
   * @param port
   * @return true, if port is valid
   */
  public static boolean isValidPort(int port) {
    if (port >= 1 && port <= 65535) {
      return true;
    }
    return false;
  }

  public static File createTempDir() {
    return createTempDir(new File(System.getProperty("java.io.tmpdir")));
  }

  public static File createTempDir(File parent) {
    File temp =
        new File(parent,
            Integer.toString(Math.abs(RANDOM.nextInt()) % 100000000));
    temp.delete();
    temp.mkdir();
    temp.deleteOnExit();
    return temp;
  }

  public static void zip(File input, File output) throws IOException {
    FileOutputStream out = new FileOutputStream(output);
    ZipOutputStream zOut = new ZipOutputStream(out);
    try {
      zipFile("", input, zOut);
    } finally {
      zOut.close();
    }
  }

  public static void zipFolderContent(File folder, File output)
      throws IOException {
    FileOutputStream out = new FileOutputStream(output);
    ZipOutputStream zOut = new ZipOutputStream(out);
    try {
      File[] files = folder.listFiles();
      if (files != null) {
        for (File f : files) {
          zipFile("", f, zOut);
        }
      }
    } finally {
      zOut.close();
    }
  }

  private static void zipFile(String path, File input, ZipOutputStream zOut)
      throws IOException {
    if (input.isDirectory()) {
      File[] files = input.listFiles();
      if (files != null) {
        for (File f : files) {
          String childPath =
              path + input.getName() + (f.isDirectory() ? "/" : "");
          zipFile(childPath, f, zOut);
        }
      }
    } else {
      String childPath =
          path + (path.length() > 0 ? "/" : "") + input.getName();
      ZipEntry entry = new ZipEntry(childPath);
      zOut.putNextEntry(entry);
      InputStream fileInputStream =
          new BufferedInputStream(new FileInputStream(input));
      try {
        IOUtils.copy(fileInputStream, zOut);
      } finally {
        fileInputStream.close();
      }
    }
  }

  public static void unzip(ZipFile source, File dest) throws IOException {
    Enumeration<?> entries = source.entries();
    while (entries.hasMoreElements()) {
      ZipEntry entry = (ZipEntry) entries.nextElement();
      File newFile = new File(dest, entry.getName());
      if (entry.isDirectory()) {
        newFile.mkdirs();
      } else {
        newFile.getParentFile().mkdirs();
        InputStream src = source.getInputStream(entry);
        try {
          OutputStream output =
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

  public static String flattenToString(Collection<?> collection,
      String delimiter) {
    StringBuffer buffer = new StringBuffer();
    for (Object obj : collection) {
      buffer.append(obj.toString());
      buffer.append(',');
    }

    if (buffer.length() > 0) {
      buffer.setLength(buffer.length() - 1);
    }
    return buffer.toString();
  }

  public static Double convertToDouble(Object obj) {
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
  private static RuntimeException getCause(InvocationTargetException e) {
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException)
      throw (RuntimeException) cause;
    else
      throw new IllegalStateException(e.getCause());
  }

  /**
   * Get the Class of all the objects
   *
   * @param args The objects to get the Classes from
   * @return The classes as an array
   */
  public static Class<?>[] getTypes(Object... args) {
    Class<?>[] argTypes = new Class<?>[args.length];
    for (int i = 0; i < argTypes.length; i++)
      argTypes[i] = args[i].getClass();
    return argTypes;
  }

  public static Object callConstructor(Class<?> c, Object... args) {
    return callConstructor(c, getTypes(args), args);
  }

  /**
   * Call the class constructor with the given arguments
   *
   * @param c The class
   * @param args The arguments
   * @return The constructed object
   */
  public static Object callConstructor(Class<?> c, Class<?>[] argTypes,
      Object[] args) {
    try {
      Constructor<?> cons = c.getConstructor(argTypes);
      return cons.newInstance(args);
    } catch (InvocationTargetException e) {
      throw getCause(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(e);
    } catch (InstantiationException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String formatDuration(long startTime, long endTime) {
    if (startTime == -1) {
      return "-";
    }

    long durationMS;
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

    long days = hours / 24;
    hours %= 24;
    return days + "d " + hours + "h " + minutes + "m";
  }

  public static Object invokeStaticMethod(ClassLoader loader, String className,
      String methodName, Object... args) throws ClassNotFoundException,
      SecurityException, NoSuchMethodException, IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    Class<?> clazz = loader.loadClass(className);

    Class<?>[] argTypes = new Class[args.length];
    for (int i = 0; i < args.length; ++i) {
      // argTypes[i] = args[i].getClass();
      argTypes[i] = args[i].getClass();
    }

    Method method = clazz.getDeclaredMethod(methodName, argTypes);
    return method.invoke(null, args);
  }

  public static void copyStream(InputStream input, OutputStream output)
      throws IOException {
    byte[] buffer = new byte[1024];
    int bytesRead;
    while ((bytesRead = input.read(buffer)) != -1) {
      output.write(buffer, 0, bytesRead);
    }
  }

  public static ReadablePeriod parsePeriodString(String periodStr) {
    ReadablePeriod period;
    char periodUnit = periodStr.charAt(periodStr.length() - 1);
    if (periodStr.equals("null") || periodUnit == 'n') {
      return null;
    }

    int periodInt =
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

  public static String createPeriodString(ReadablePeriod period) {
    String periodStr = "null";

    if (period == null) {
      return "null";
    }

    if (period.get(DurationFieldType.years()) > 0) {
      int years = period.get(DurationFieldType.years());
      periodStr = years + "y";
    } else if (period.get(DurationFieldType.months()) > 0) {
      int months = period.get(DurationFieldType.months());
      periodStr = months + "M";
    } else if (period.get(DurationFieldType.weeks()) > 0) {
      int weeks = period.get(DurationFieldType.weeks());
      periodStr = weeks + "w";
    } else if (period.get(DurationFieldType.days()) > 0) {
      int days = period.get(DurationFieldType.days());
      periodStr = days + "d";
    } else if (period.get(DurationFieldType.hours()) > 0) {
      int hours = period.get(DurationFieldType.hours());
      periodStr = hours + "h";
    } else if (period.get(DurationFieldType.minutes()) > 0) {
      int minutes = period.get(DurationFieldType.minutes());
      periodStr = minutes + "m";
    } else if (period.get(DurationFieldType.seconds()) > 0) {
      int seconds = period.get(DurationFieldType.seconds());
      periodStr = seconds + "s";
    }

    return periodStr;
  }

  /**
   * @param strMemSize : memory string in the format such as 1G, 500M, 3000K, 5000
   * @return : long value of memory amount in kb
   */
  public static long parseMemString(String strMemSize) {
    if (strMemSize == null) {
      return 0L;
    }

    long size = 0L;
    if (strMemSize.endsWith("g") || strMemSize.endsWith("G")
        || strMemSize.endsWith("m") || strMemSize.endsWith("M")
        || strMemSize.endsWith("k") || strMemSize.endsWith("K")) {
      String strSize = strMemSize.substring(0, strMemSize.length() - 1);
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
   * @param cronExpression: A cron expression is a string separated by white space, to provide a parser and evaluator for Quartz cron expressions.
   * @return : org.quartz.CronExpression object.
   *
   * TODO: Currently, we have to transform Joda Timezone to Java Timezone due to CronExpression.
   *       Since Java8 enhanced Time functionalities, We consider transform all Jodatime to Java Time in future.
   *
   */
  public static CronExpression parseCronExpression(String cronExpression, DateTimeZone timezone) {
    if (cronExpression != null) {
      try {
        CronExpression ce =  new CronExpression(cronExpression);
        ce.setTimeZone(TimeZone.getTimeZone(timezone.getID()));
        return ce;
      } catch (ParseException pe) {
        logger.error("this cron expression {" + cronExpression + "} can not be parsed. "
            + "Please Check Quartz Cron Syntax.");
      }
      return null;
    } else return null;
  }

  public static boolean isCronExpressionValid(String cronExpression) {
    return CronExpression.isValidExpression(cronExpression);
  }
}
