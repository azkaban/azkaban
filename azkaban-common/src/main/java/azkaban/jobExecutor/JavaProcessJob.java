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

import azkaban.project.DirectoryFlowLoader;
import azkaban.server.AzkabanServer;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    public static String JAR_DIR = "/tmp/jars/";


    protected Configuration conf = new Configuration();

    public static final String HADOOP_CONF_DIR_PROP = "hadoop.conf.dir"; // hadoop dir with xml conf files. from azkaban private.props
    public static final String HADOOP_INJECT_MASTER_IP = "hadoop-inject." + "hadoop.master.ip";


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

    /**
     * Set up hadoop configs for hadoopclient
     */
    protected void setHadoopConfigs() throws IOException {
        ArrayList<URL> resources = new ArrayList<>();
        resources.add((new File(getSysProps().get(HADOOP_CONF_DIR_PROP))).toURI().toURL());

        conf = new Configuration();

        if (jobProps.containsKey(HADOOP_INJECT_MASTER_IP)) {
            conf.set("hadoop.master.ip", jobProps.getString(HADOOP_INJECT_MASTER_IP));
        }
    }



    /**
     * Utility function for loading files from S3/local
     *
     * @param paths
     * @return
     */
    protected List<String> getFromLocalOrS3Concurrent(List<String> paths) throws RuntimeException {
        ConcurrentMap<String, String> classpathMap = new ConcurrentHashMap<>();
        // Download the file from S3 URL when
        // case 1: if the file doesn't exist locally
        // case 2: the file exists locally but it's different from S3

        try {

            for (String path : paths) {
                File file = new File(path);

                // check if the path is a local url)
                if (file.exists()) {
                    classpathMap.put(path, path);
                } else {
                    URI input_path = new URI(path);
                    // get file path as key
                    String key = input_path.getPath();

                    String localPath = JAR_DIR + key;
                    // if it's a s3 path
                    if (input_path.getScheme() != null && input_path.getScheme().startsWith("s3")) {
                        // set up the hadoop configs for hadoop file system
                        setHadoopConfigs();

                        // remove the first letter if it starts with /
                        if (key.startsWith("/")) {
                            key = key.substring(1);
                        }

                        localPath = JAR_DIR + key;

                        File localFile = new File(localPath);
                        // getLog().info("path is: s3a://" + input_path.getHost() + input_path.getPath());
                        Path s3Path = new Path("s3a://" + input_path.getHost() + input_path.getPath());
                        FileSystem s3Fs = s3Path.getFileSystem(conf);

                        if (!localFile.exists()) {
                            s3Fs.copyToLocalFile(s3Path, new Path(localPath));
                        } else {
                            // Check the length of file
                            // TODO: (ideally this should be MD5 value, need to check how much time it needs to use)
                            if (s3Fs.getContentSummary(s3Path).getLength() != localFile.length()) {
                                getLog().info("Updated file: " + key);
                                s3Fs.copyToLocalFile(s3Path, new Path(localPath));
                            }
                        }

                        classpathMap.put(localPath, path);

                    } else {
                        if (new File(localPath).exists()) {
                            classpathMap.put(localPath, path);
                        }
                    }
                }
            }

        } catch (URISyntaxException e1) {
            getLog().error("URI syntax exception");
        } catch (IOException e2) {
            getLog().error("IO exception");
        }

        // if nothing is added, return the input paths
        if (classpathMap.isEmpty()) {
            return paths;
        } else {
            return new ArrayList<>(classpathMap.keySet());
        }
    }

    protected List<String> getClassPaths() {
        List<String> classPaths = getJobProps().getStringList(CLASSPATH, null, ",");

        ArrayList<String> classpathList = new ArrayList<>();

        // Adding global properties used system wide.
        if (getJobProps().containsKey(GLOBAL_CLASSPATH)) {
            List<String> globalClasspath = getJobProps().getStringList(GLOBAL_CLASSPATH);
            for (String global : globalClasspath) {
                getLog().info("Adding to global classpath:" + global);
                // the original way
                if (classPaths != null && ! classPaths.isEmpty()) {
                    classpathList.add(global);
                } else {
                    // if the class paths are defined, need to add individual jars to the path
                    global = global.substring(0, global.lastIndexOf("/"));
                    File globalDir = new File(global);
                    if (globalDir.exists() && globalDir.isDirectory()) {
                        File[] files = globalDir.listFiles();
                        if (files != null && files.length > 0) {
                            for (File file : files) {
                                if (file.isFile() && file.getName().endsWith(".jar")) {
                                    classpathList.add(file.getAbsolutePath());
                                }
                            }
                        }
                    }
                }
            }
        }

        File path = new File(getPath());
//    // File parent = path.getParentFile();

        // This is where external class paths (e.g. on s3) specified in properties/job files to get loaded to Azkaban
        if (classPaths != null) {
            getLog().info("Found additional class paths. Loading class paths from S3 to azkaban");
            List<String> pathList = getFromLocalOrS3Concurrent(classPaths);
            classpathList.addAll(pathList);
            getLog().info("classpath output: " + classpathList);
        } else {
            getLog().info("No classpath specified. Trying to load classes from " + path);
        }

        if (path.exists()) {
            for (File file : path.listFiles()) {
                if (file.getName().endsWith(".jar")) {
                    // log.info("Adding to classpath:" + file.getName());
                    classpathList.add(file.getAbsolutePath());
                }
            }
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
