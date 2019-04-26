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

import azkaban.jobtype.MapReduceJobState;
import azkaban.jobtype.StatsUtils;
import azkaban.utils.Props;
import azkaban.utils.JSONUtils;
import java.io.IOException;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static azkaban.security.commons.SecurityUtils.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;


public abstract class AbstractHadoopJob {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHadoopJob.class);

  public static String COMMON_FILE_DATE_PATTERN = "yyyy-MM-dd-HH-mm";
  public static final String HADOOP_PREFIX = "hadoop-conf.";

  private RunningJob runningJob;
  private final Props props;
  private final String jobName;

  private JobConf jobconf;
  private JobClient jobClient;
  private Configuration conf;

  private boolean visualizer;
  private MapReduceJobState mapReduceJobState;
  private String jobStatsFileName;

  public AbstractHadoopJob(String name, Props props) {
    this.props = props;
    this.jobName = name;
    conf = new Configuration();
    jobconf = new JobConf(conf);
    jobconf.setJobName(name);

    visualizer = props.getBoolean("mr.listener.visualizer", false) == true;
    if (visualizer == true) {
      jobStatsFileName = props.getString("azkaban.job.attachment.file");
    }
  }

  public JobConf getJobConf() {
    return jobconf;
  }

  public Configuration getConf() {
    return conf;
  }

  public String getJobName() {
    return this.jobName;
  }

  public void run() throws Exception {
    JobConf conf = getJobConf();

    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    jobClient = new JobClient(conf);
    runningJob = jobClient.submitJob(conf);
    LOG.info("See " + runningJob.getTrackingURL() + " for details.");
    jobClient.monitorAndPrintJob(conf, runningJob);

    if (!runningJob.isSuccessful()) {
      throw new Exception("Hadoop job:" + getJobName() + " failed!");
    }

    // dump all counters
    Counters counters = runningJob.getCounters();
    for (String groupName : counters.getGroupNames()) {
      Counters.Group group = counters.getGroup(groupName);
      LOG.info("Group: " + group.getDisplayName());
      for (Counter counter : group) {
        LOG.info(counter.getDisplayName() + ":\t" + counter.getValue());
      }
    }
    updateMapReduceJobState(conf);
  }

  @SuppressWarnings("rawtypes")
  public JobConf createJobConf(Class<? extends Mapper> mapperClass)
      throws IOException, URISyntaxException {
    JobConf conf = createJobConf(mapperClass, null);
    conf.setNumReduceTasks(0);
    return conf;
  }

  @SuppressWarnings("rawtypes")
  public JobConf createJobConf(Class<? extends Mapper> mapperClass,
      Class<? extends Reducer> reducerClass,
      Class<? extends Reducer> combinerClass) throws IOException,
      URISyntaxException {
    JobConf conf = createJobConf(mapperClass, reducerClass);
    conf.setCombinerClass(combinerClass);
    return conf;
  }

  @SuppressWarnings("rawtypes")
  public JobConf createJobConf(Class<? extends Mapper> mapperClass,
      Class<? extends Reducer> reducerClass) throws IOException,
      URISyntaxException {
    JobConf conf = new JobConf();
    // set custom class loader with custom find resource strategy.

    conf.setJobName(getJobName());
    conf.setMapperClass(mapperClass);
    if (reducerClass != null) {
      conf.setReducerClass(reducerClass);
    }

    if (props.getBoolean("is.local", false)) {
      conf.set("mapred.job.tracker", "local");
      conf.set("fs.default.name", "file:///");
      conf.set("mapred.local.dir", "/tmp/map-red");

      LOG.info("Running locally, no hadoop jar set.");
    } else {
      HadoopUtils.setClassLoaderAndJar(conf, getClass());
      LOG.info("Setting hadoop jar file for class:" + getClass()
          + "  to " + conf.getJar());
      LOG.info("*************************************************************************");
      LOG.info("          Running on Real Hadoop Cluster("
          + conf.get("mapred.job.tracker") + ")           ");
      LOG.info("*************************************************************************");
    }

    // set JVM options if present
    if (props.containsKey("mapred.child.java.opts")) {
      conf.set("mapred.child.java.opts",
          props.getString("mapred.child.java.opts"));
      LOG.info("mapred.child.java.opts set to "
          + props.getString("mapred.child.java.opts"));
    }

    // set input and output paths if they are present
    if (props.containsKey("input.paths")) {
      List<String> inputPaths = props.getStringList("input.paths");
      if (inputPaths.size() == 0) {
        throw new IllegalArgumentException(
            "Must specify at least one value for property 'input.paths'");
      }
      for (String path : inputPaths) {
        HadoopUtils.addAllSubPaths(conf, new Path(path));
      }
    }

    if (props.containsKey("output.path")) {
      String location = props.get("output.path");
      FileOutputFormat.setOutputPath(conf, new Path(location));

      // For testing purpose only remove output file if exists
      if (props.getBoolean("force.output.overwrite", false)) {
        FileSystem fs =
            FileOutputFormat.getOutputPath(conf).getFileSystem(conf);
        fs.delete(FileOutputFormat.getOutputPath(conf), true);
      }
    }

    // Adds External jars to hadoop classpath
    String externalJarList = props.getString("hadoop.external.jarFiles", null);
    if (externalJarList != null) {
      FileSystem fs = FileSystem.get(conf);
      String[] jarFiles = externalJarList.split(",");
      for (String jarFile : jarFiles) {
        LOG.info("Adding extenral jar File:" + jarFile);
        DistributedCache.addFileToClassPath(new Path(jarFile), conf, fs);
      }
    }

    // Adds distributed cache files
    String cacheFileList = props.getString("hadoop.cache.files", null);
    if (cacheFileList != null) {
      String[] cacheFiles = cacheFileList.split(",");
      for (String cacheFile : cacheFiles) {
        LOG.info("Adding Distributed Cache File:" + cacheFile);
        DistributedCache.addCacheFile(new URI(cacheFile), conf);
      }
    }

    // Adds distributed cache files
    String archiveFileList = props.getString("hadoop.cache.archives", null);
    if (archiveFileList != null) {
      String[] archiveFiles = archiveFileList.split(",");
      for (String archiveFile : archiveFiles) {
        LOG.info("Adding Distributed Cache Archive File:" + archiveFile);
        DistributedCache.addCacheArchive(new URI(archiveFile), conf);
      }
    }

    String hadoopCacheJarDir =
        props.getString("hdfs.default.classpath.dir", null);
    if (hadoopCacheJarDir != null) {
      FileSystem fs = FileSystem.get(conf);
      if (fs != null) {
        FileStatus[] status = fs.listStatus(new Path(hadoopCacheJarDir));

        if (status != null) {
          for (int i = 0; i < status.length; ++i) {
            if (!status[i].isDir()) {
              Path path =
                  new Path(hadoopCacheJarDir, status[i].getPath().getName());
              LOG.info("Adding Jar to Distributed Cache Archive File:"
                  + path);

              DistributedCache.addFileToClassPath(path, conf, fs);
            }
          }
        } else {
          LOG.info("hdfs.default.classpath.dir " + hadoopCacheJarDir
              + " is empty.");
        }
      } else {
        LOG.info("hdfs.default.classpath.dir " + hadoopCacheJarDir
            + " filesystem doesn't exist");
      }
    }

    for (String key : getProps().getKeySet()) {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_PREFIX)) {
        String newKey = key.substring(HADOOP_PREFIX.length());
        conf.set(newKey, getProps().get(key));
      }
    }

    HadoopUtils.setPropsInJob(conf, getProps());

    // put in tokens
    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY,
          System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    return conf;
  }

  public Props getProps() {
    return this.props;
  }

  public void cancel() throws Exception {
    if (runningJob != null) {
      runningJob.killJob();
    }
  }

  private void updateMapReduceJobState(JobConf jobConf) {
    if (runningJob == null || visualizer == false) {
      return;
    }

    try {
      JobID jobId = runningJob.getID();
      TaskReport[] mapTaskReport = jobClient.getMapTaskReports(jobId);
      TaskReport[] reduceTaskReport = jobClient.getReduceTaskReports(jobId);
      mapReduceJobState =
          new MapReduceJobState(runningJob, mapTaskReport, reduceTaskReport);
      writeMapReduceJobState(jobConf);
    } catch (IOException e) {
      LOG.error("Cannot update MapReduceJobState");
    }
  }

  private Object statsToJson(JobConf jobConf) {
    List<Object> jsonObj = new ArrayList<Object>();
    Map<String, Object> jobJsonObj = new HashMap<String, Object>();
    Properties conf = StatsUtils.getJobConf(jobConf);
    jobJsonObj.put("state", mapReduceJobState.toJson());
    jobJsonObj.put("conf", StatsUtils.propertiesToJson(conf));
    jsonObj.add(jobJsonObj);
    return jsonObj;
  }

  private void writeMapReduceJobState(JobConf jobConf) {
    File mrStateFile = null;
    try {
      mrStateFile = new File(jobStatsFileName);
      JSONUtils.toJSON(statsToJson(jobConf), mrStateFile);
    } catch (Exception e) {
      LOG.error("Cannot write JSON file.");
    }
  }

  public double getProgress() throws IOException {
    if (runningJob == null) {
      return 0.0;
    }
    return (double) (runningJob.mapProgress() + runningJob.reduceProgress()) / 2.0d;
  }

  public Counters getCounters() throws IOException {
    return runningJob.getCounters();
  }

}
