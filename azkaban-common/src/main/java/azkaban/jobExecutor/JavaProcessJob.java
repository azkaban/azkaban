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

package azkaban.jobExecutor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.log4j.Logger;

import azkaban.project.DirectoryFlowLoader;
import azkaban.server.AzkabanServer;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class JavaProcessJob extends ProcessJob {
  public static final String CLASSPATH = "classpath";
  public static final String GLOBAL_CLASSPATH = "global.classpaths";
  public static final String JAVA_CLASS = "java.class";
  public static final String INITIAL_MEMORY_SIZE = "Xms";
  public static final String MAX_MEMORY_SIZE = "Xmx";
  public static final String MAIN_ARGS = "main.args";
  public static final String JVM_PARAMS = "jvm.args";
  public static final String GLOBAL_JVM_PARAMS = "global.jvm.args";

  public static final String DEFAULT_INITIAL_MEMORY_SIZE = "64M";
  public static final String DEFAULT_MAX_MEMORY_SIZE = "256M";

  public static String JAVA_COMMAND = "java";
  // jar directory
  public static String JAR_DIR = "/solo/jars";

  protected Configuration conf = new Configuration();

  public JavaProcessJob(String jobid, Props sysProps, Props jobProps,
      Logger logger) {
    super(jobid, sysProps, jobProps, logger);
  }

  @Override
  protected List<String> getCommandList() {
    ArrayList<String> list = new ArrayList<String>();
    list.add(createCommandLine());
    return list;
  }

  protected String createCommandLine() {
    String command = JAVA_COMMAND + " ";
    command += getJVMArguments() + " ";
    command += "-Xms" + getInitialMemorySize() + " ";
    command += "-Xmx" + getMaxMemorySize() + " ";
    command += "-cp " + createArguments(getClassPaths(), ":") + " ";
    command += getJavaClass() + " ";
    command += getMainArguments();

    return command;
  }

  protected String getJavaClass() {
    return getJobProps().getString(JAVA_CLASS);
  }

  protected String getClassPathParam() {
    List<String> classPath = getClassPaths();
    if (classPath == null || classPath.size() == 0) {
      return "";
    }

    return "-cp " + createArguments(classPath, ":") + " ";
  }


  protected AmazonS3Client getAWSClient() {
    return new AmazonS3Client(getAWSCredentials()).withRegion(Regions.US_WEST_2);
  }

  protected AWSCredentials getAWSCredentials() {
    DefaultAWSCredentialsProviderChain providerChain = new DefaultAWSCredentialsProviderChain();
    return providerChain.getCredentials();
  }

  /**
   * Concurrent map version implementation
   *
   * @param paths
   * @return
   */
  protected List<String> getFromLocalOrS3Concurrent(List<String> paths) {
    ConcurrentMap<String, String> classpathMap = new ConcurrentHashMap<>();
    for (String path: paths) {
      File file = new File(path);
      String fileName = file.getName();
      getLog().info("filename:" + file.getPath());

      if (file.exists()) {
        classpathMap.put(fileName, path);
      } else {
        try {
          URI s3path = new URI(path);
          String bucket = s3path.getHost();
          String key = s3path.getPath(); // baseUrl.substring(index, lastIndex);
          String localPath = JAR_DIR + key;
          getLog().info("test path: " + fileName);
          getLog().info("test scheme: " + s3path.getScheme());
          // if it's a s3 path
          if (s3path.getScheme() != null && s3path.getScheme().startsWith("s3")) {
            getLog().info("s3 address: " + s3path);
            getLog().info("bucket name: " + bucket);
            getLog().info("key name: " + key);
            S3Object obj = getAWSClient().getObject(new GetObjectRequest(bucket, key));
            // write the file to JAR_DIR
            InputStream objectData = obj.getObjectContent();
            Files.copy(objectData, new File(localPath).toPath());
            objectData.close();

            // alternative s3path method
//            Path downloadPath = new Path(path);
//            FileSystem s3Fs = downloadPath.getFileSystem(conf);
//            s3Fs.copyToLocalFile(downloadPath, new Path(localPath));

            // write the file to JAR_DIR
            classpathMap.put(localPath, path);
          } else {
            if (new File(localPath).exists()) {
              classpathMap.put(localPath, path);
            }
          }
        } catch (URISyntaxException e2) {
          getLog().error("URI syntax exception");
        } catch (IOException e2) {
          getLog().error("IO exception");
        }

      }

    }
    if (classpathMap.isEmpty()) {
      return paths;
    } else {
      return new ArrayList<>(classpathMap.keySet());
    }
  }

  /**
   * helper function to download stuff from S3 & return the valid class paths
   *
   * @param paths
   */
  protected List<String> getFromLocalOrS3(List<String> paths) {
    ArrayList<String> classPaths = new ArrayList<>();

    for (String path : paths) {
      File file = new File(path);
      String fileName = file.getName();
      getLog().info("filename:" + file.getPath());

      if (file.exists()) {
        classPaths.add(fileName);
      } else {
        try {
          URI s3path = new URI(path);
          String bucket = s3path.getHost();
          String key = s3path.getPath(); // baseUrl.substring(index, lastIndex);
          if (key.startsWith("/")) {
            key = key.substring(1);
          }
          getLog().info("test path: " + fileName);
          getLog().info("test scheme: " + s3path.getScheme());
          // if it's a s3 path
          if (s3path.getScheme() != null && s3path.getScheme().startsWith("s3")) {
            getLog().info("s3 address: " + s3path);
            getLog().info("bucket name: " + bucket);
            getLog().info("key name: " + key);
            S3Object obj = getAWSClient().getObject(new GetObjectRequest(bucket, key));
            // write the file to JAR_DI
            InputStream objectData = obj.getObjectContent();
            Files.copy(objectData, new File(JAR_DIR + fileName).toPath());
            objectData.close();
            // write the file to JAR_DIR
            classPaths.add(JAR_DIR + key);
          } else {
            if (new File(JAR_DIR + key).exists()) {
              classPaths.add(JAR_DIR + key);
            }
          }
        } catch (URISyntaxException e2) {
          getLog().error("URI syntax exception");
        } catch (IOException e2) {
          getLog().error("IO exception");
        }

      }
    }

    if (classPaths.isEmpty()) {
      return paths;
    } else
      return classPaths;

  }

  protected List<String> getClassPaths() {

    List<String> classPaths = getJobProps().getStringList(CLASSPATH, null, ",");

    ArrayList<String> classpathList = new ArrayList<>();
    // Adding global properties used system wide.
    if (getJobProps().containsKey(GLOBAL_CLASSPATH)) {
      List<String> globalClasspath =
          getJobProps().getStringList(GLOBAL_CLASSPATH);
      for (String global : globalClasspath) {
        getLog().info("Adding to global classpath:" + global);
        classpathList.add(global);
      }
    }

    if (classPaths == null) {
      File path = new File(getPath());
      // File parent = path.getParentFile();
      getLog().info(
          "No classpath specified. Trying to load classes from " + path);

      if (path != null) {
        for (File file : path.listFiles()) {
          if (file.getName().endsWith(".jar")) {
            // log.info("Adding to classpath:" + file.getName());
            classpathList.add(file.getName());
          }
        }
      }
    } else {
      // TODO: WIP: convert s3 path to local path & cache new jars
      List<String> pathList = getFromLocalOrS3Concurrent(classPaths);
      getLog().info("TESTING: " + pathList);
      classpathList.addAll(getFromLocalOrS3(pathList));
    }

    return classpathList;
  }

  protected String getInitialMemorySize() {
    return getJobProps().getString(INITIAL_MEMORY_SIZE,
        DEFAULT_INITIAL_MEMORY_SIZE);
  }

  protected String getMaxMemorySize() {
    return getJobProps().getString(MAX_MEMORY_SIZE, DEFAULT_MAX_MEMORY_SIZE);
  }

  protected String getMainArguments() {
    return getJobProps().getString(MAIN_ARGS, "");
  }

  protected String getJVMArguments() {
    String globalJVMArgs = getJobProps().getString(GLOBAL_JVM_PARAMS, null);

    if (globalJVMArgs == null) {
      return getJobProps().getString(JVM_PARAMS, "");
    }

    return globalJVMArgs + " " + getJobProps().getString(JVM_PARAMS, "");
  }

  protected String createArguments(List<String> arguments, String separator) {
    if (arguments != null && arguments.size() > 0) {
      String param = "";
      for (String arg : arguments) {
        param += arg + separator;
      }

      return param.substring(0, param.length() - 1);
    }

    return "";
  }

  protected Pair<Long, Long> getProcMemoryRequirement() throws Exception {
    String strXms = getInitialMemorySize();
    String strXmx = getMaxMemorySize();
    long xms = Utils.parseMemString(strXms);
    long xmx = Utils.parseMemString(strXmx);

    Props azkabanProperties = AzkabanServer.getAzkabanProperties();
    if (azkabanProperties != null) {
      String maxXms = azkabanProperties.getString(DirectoryFlowLoader.JOB_MAX_XMS, DirectoryFlowLoader.MAX_XMS_DEFAULT);
      String maxXmx = azkabanProperties.getString(DirectoryFlowLoader.JOB_MAX_XMX, DirectoryFlowLoader.MAX_XMX_DEFAULT);
      long sizeMaxXms = Utils.parseMemString(maxXms);
      long sizeMaxXmx = Utils.parseMemString(maxXmx);

      if (xms > sizeMaxXms) {
        throw new Exception(String.format("%s: Xms value has exceeded the allowed limit (max Xms = %s)",
                getId(), maxXms));
      }

      if (xmx > sizeMaxXmx) {
        throw new Exception(String.format("%s: Xmx value has exceeded the allowed limit (max Xmx = %s)",
                getId(), maxXmx));
      }
    }

    return new Pair<Long, Long>(xms, xmx);
  }
}
