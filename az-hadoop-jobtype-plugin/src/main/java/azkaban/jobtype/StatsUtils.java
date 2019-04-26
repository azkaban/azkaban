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
package azkaban.jobtype;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.pig.impl.util.ObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatsUtils {

  private static Logger LOG = LoggerFactory.getLogger(StatsUtils.class);

  private static final Set<String> JOB_CONF_KEYS = new HashSet<String>(
      Arrays.asList(new String[]{
          "mapred.job.map.memory.mb",
          "mapred.job.reduce.memory.mb",
          "mapred.child.java.opts",
          "mapred.cache.files",
          "mapred.cache.archives",
          "mapred.cache.files.filesizes",
          "mapred.min.split.size",
          "mapred.max.split.size",
          "mapred.output.compress",
          "mapred.output.compression.type",
          "mapred.output.compression.codec",
          "mapred.compress.map.output",
          "mapred.map.output.compression.codec",
          "mapred.queue.names",
          "mapred.job.queue.name",
          "io.sort.mb"
      }));

  public static Properties getJobConf(RunningJob runningJob) {
    try {
      Path path = new Path(runningJob.getJobFile());
      Configuration conf = new Configuration(false);
      FileSystem fs = FileSystem.get(new Configuration());
      InputStream in = fs.open(path);
      conf.addResource(in);
      return getJobConf(conf);
    } catch (FileNotFoundException e) {
      LOG.warn("Job conf not found.");
    } catch (IOException e) {
      LOG.warn("Error while retrieving job conf: " + e.getMessage());
    }
    return null;
  }

  public static Properties getJobConf(Configuration conf) {
    if (conf == null) {
      return null;
    }

    Properties jobConfProperties = null;
    try {
      jobConfProperties = new Properties();
      for (Map.Entry<String, String> entry : conf) {
        if (entry.getKey().equals("pig.mapPlan")
            || entry.getKey().equals("pig.reducePlan")) {
          jobConfProperties.setProperty(entry.getKey(), ObjectSerializer
              .deserialize(entry.getValue()).toString());
        } else if (JOB_CONF_KEYS.contains(entry.getKey())) {
          jobConfProperties.setProperty(entry.getKey(), entry.getValue());
        }
      }
    } catch (IOException e) {
      LOG.warn("Error while reading job conf: " + e.getMessage());
    }
    return jobConfProperties;
  }

  public static Object propertiesToJson(Properties properties) {
    Map<String, String> jsonObj = new HashMap<String, String>();
    if (properties != null) {
      Set<String> keys = properties.stringPropertyNames();
      for (String key : keys) {
        jsonObj.put(key, properties.getProperty(key));
      }
    }
    return jsonObj;
  }

  public static Properties propertiesFromJson(Object obj) {
    @SuppressWarnings("unchecked")
    Map<String, String> jsonObj = (HashMap<String, String>) obj;

    Properties properties = new Properties();
    for (Map.Entry<String, String> entry : jsonObj.entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
    return properties;
  }

  public static Object countersToJson(Counters counters) {
    Map<String, Object> jsonObj = new HashMap<String, Object>();

    if (counters == null) {
      return jsonObj;
    }

    Collection<String> counterGroups = counters.getGroupNames();
    for (String groupName : counterGroups) {
      Map<String, String> counterStats = new HashMap<String, String>();
      Group group = counters.getGroup(groupName);
      Iterator<Counter> it = group.iterator();
      while (it.hasNext()) {
        Counter counter = it.next();
        counterStats.put(counter.getDisplayName(),
            String.valueOf(counter.getCounter()));
      }
      jsonObj.put(groupName, counterStats);
    }
    return jsonObj;
  }
}
