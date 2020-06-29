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
package azkaban.jobtype.javautils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import azkaban.utils.Props;


/**
 * Utilities of Hadoop Related Operations
 */
public class HadoopUtils {

  private static final Logger logger = Logger.getLogger(HadoopUtils.class);

  public static void setClassLoaderAndJar(JobConf conf, Class<?> jobClass) {
    conf.setClassLoader(Thread.currentThread().getContextClassLoader());
    String jar =
        findContainingJar(jobClass, Thread.currentThread()
            .getContextClassLoader());
    if (jar != null) {
      conf.setJar(jar);
    }
  }

  public static String findContainingJar(String fileName, ClassLoader loader) {
    try {
      for (Enumeration<?> itr = loader.getResources(fileName); itr
          .hasMoreElements(); ) {
        URL url = (URL) itr.nextElement();
        logger.info("findContainingJar finds url:" + url);
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public static String findContainingJar(Class<?> my_class, ClassLoader loader) {
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    return findContainingJar(class_file, loader);
  }

  public static boolean shouldPathBeIgnored(Path path) {
    return path.getName().startsWith("_");
  }

  public static JobConf addAllSubPaths(JobConf conf, Path path)
      throws IOException {
    if (shouldPathBeIgnored(path)) {
      throw new IllegalArgumentException(String.format(
          "Path[%s] should be ignored.", path));
    }

    final FileSystem fs = path.getFileSystem(conf);

    if (fs.exists(path)) {
      for (FileStatus status : fs.listStatus(path)) {
        if (!shouldPathBeIgnored(status.getPath())) {
          if (status.isDir()) {
            addAllSubPaths(conf, status.getPath());
          } else {
            FileInputFormat.addInputPath(conf, status.getPath());
          }
        }
      }
    }
    return conf;
  }

  public static void setPropsInJob(Configuration conf, Props props) {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      props.storeFlattened(output);
      conf.set("azkaban.props", new String(output.toByteArray(), "UTF-8"));
    } catch (IOException e) {
      throw new RuntimeException("This is not possible!", e);
    }
  }

  public static void saveProps(Props props, String file) throws IOException {
    Path path = new Path(file);
    FileSystem fs = path.getFileSystem(new Configuration());
    saveProps(fs, props, file);
  }

  public static void saveProps(FileSystem fs, Props props, String file)
      throws IOException {
    Path path = new Path(file);

    // create directory if it does not exist.
    Path parent = path.getParent();
    if (!fs.exists(parent)) {
      fs.mkdirs(parent);
    }

    // write out properties
    OutputStream output = fs.create(path);
    try {
      props.storeFlattened(output);
    } finally {
      output.close();
    }
  }
}
