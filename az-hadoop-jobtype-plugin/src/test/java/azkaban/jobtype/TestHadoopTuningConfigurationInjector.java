/*
 * Copyright 2018 LinkedIn Corp.
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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Assert;
import org.junit.Test;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;

import com.google.common.io.Files;


public class TestHadoopTuningConfigurationInjector {

  @Test
  public void testPrepareResourcesToInject() throws IOException {

    File root = Files.createTempDir();

    Props allProps = new Props();
    Props azkabanProps = new Props();
    Props userProps = new Props();

    userProps.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB, "100");
    userProps.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_FACTOR, "100");
    userProps.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB, "2048");
    userProps.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB, "2048");

    String[] propsToInject = new String[]{
        CommonJobProperties.EXEC_ID,
        CommonJobProperties.FLOW_ID,
        CommonJobProperties.JOB_ID,
        CommonJobProperties.PROJECT_NAME,
        CommonJobProperties.PROJECT_VERSION,
        CommonJobProperties.EXECUTION_LINK,
        CommonJobProperties.JOB_LINK,
        CommonJobProperties.WORKFLOW_LINK,
        CommonJobProperties.JOBEXEC_LINK,
        CommonJobProperties.ATTEMPT_LINK,
        CommonJobProperties.OUT_NODES,
        CommonJobProperties.IN_NODES,
        CommonJobProperties.PROJECT_LAST_CHANGED_DATE,
        CommonJobProperties.PROJECT_LAST_CHANGED_BY,
        CommonJobProperties.SUBMIT_USER
    };

    for (String propertyName : propsToInject) {
      azkabanProps.put(propertyName, propertyName);
    }

    allProps.putAll(userProps);
    allProps.putAll(azkabanProps);
    allProps.put("value.with.no.inject.prefix", "value");

    HadoopTuningConfigurationInjector.prepareResourcesToInject(allProps, root.getAbsolutePath());
    HadoopTuningConfigurationInjector.injectResources(allProps);

    Configuration configuration = new Configuration(true);
    configuration.addResource(new Path(HadoopConfigurationInjector.getPath(allProps, root.getAbsolutePath()) + "/"
        + HadoopTuningConfigurationInjector.INJECT_TUNING_FILE));

    Map<String, String> confProperties = userProps.getMapByPrefix(HadoopConfigurationInjector.INJECT_PREFIX);

    for (Map.Entry<String, String> entry : confProperties.entrySet()) {
      Assert.assertEquals("Value not matching in config ", configuration.get(entry.getKey()), entry.getValue());
    }
    for (String key : azkabanProps.getKeySet()) {
      Assert.assertEquals("Value not matching in config ", configuration.get(key), azkabanProps.get(key));
    }
    Assert.assertNull("Value should not be part of configuration ", configuration.get("value.with.no.inject.prefix"));

  }

}
