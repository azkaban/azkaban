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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import azkaban.jobtype.tuning.TuningCommonConstants;
import azkaban.jobtype.tuning.TuningParameterUtils;
import azkaban.utils.Props;


@RunWith(MockitoJUnitRunner.class)
public class TestTuningParameterUtils {

  @Mock
  CloseableHttpClient httpClient;

  @Test
  public void testGetHadoopProperties() throws IOException {
    Props props = new Props();
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_FACTOR, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB, "2048");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB, "2048");

    TuningParameterUtils tuningParameterUtils = new TuningParameterUtils();
    Map<String, String> confProperties = tuningParameterUtils.getHadoopProperties(props);
    Assert.assertTrue("Failed to get hadoop properties " + MRJobConfig.IO_SORT_MB,
        confProperties.containsKey(MRJobConfig.IO_SORT_MB));
    Assert.assertTrue("Failed to get right value for hadoop properties " + MRJobConfig.IO_SORT_MB,
        confProperties.get(MRJobConfig.IO_SORT_MB).equals("100"));

    Assert.assertTrue("Failed to get hadoop properties " + MRJobConfig.IO_SORT_FACTOR,
        confProperties.containsKey(MRJobConfig.IO_SORT_FACTOR));
    Assert.assertTrue("Failed to get right value for hadoop properties " + MRJobConfig.IO_SORT_FACTOR, confProperties
        .get(MRJobConfig.IO_SORT_FACTOR).equals("100"));

    Assert.assertTrue("Failed to get hadoop properties " + MRJobConfig.REDUCE_MEMORY_MB,
        confProperties.containsKey(MRJobConfig.REDUCE_MEMORY_MB));
    Assert.assertTrue("Failed to get right value for hadoop properties " + MRJobConfig.REDUCE_MEMORY_MB, confProperties
        .get(MRJobConfig.REDUCE_MEMORY_MB).equals("2048"));

    Assert.assertTrue("Failed to get hadoop properties " + MRJobConfig.MAP_MEMORY_MB,
        confProperties.containsKey(MRJobConfig.MAP_MEMORY_MB));
    Assert.assertTrue("Failed to get right value for hadoop properties " + MRJobConfig.MAP_MEMORY_MB, confProperties
        .get(MRJobConfig.MAP_MEMORY_MB).equals("2048"));
  }

  @Test
  public void testGetDefaultJobParameterJSON() throws IOException {
    Props props = new Props();
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_FACTOR, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB, "2048");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB, "2048");

    TuningParameterUtils tuningParameterUtils = new TuningParameterUtils();
    String defaultParamJSON = tuningParameterUtils.getDefaultJobParameterJSON(props);

    String expectedJSON =
        "{\"" + MRJobConfig.IO_SORT_MB + "\":\"100\"," + "\"" + MRJobConfig.MAP_MEMORY_MB + "\":\"2048\"," + "\""
            + MRJobConfig.REDUCE_MEMORY_MB + "\":\"2048\"," + "\"" + MRJobConfig.IO_SORT_FACTOR + "\":\"100\"}";

    Assert.assertEquals("Wrong JSON return for default job parameter ", defaultParamJSON, expectedJSON);
  }

  @Test
  public void testUpdateAutoTuningParameters() throws ClientProtocolException, IOException {

    String expectedJSON =
        "{\"" + MRJobConfig.IO_SORT_MB + "\":\"300\"," + "\"" + MRJobConfig.MAP_MEMORY_MB + "\":\"1024\"," + "\""
            + MRJobConfig.REDUCE_MEMORY_MB + "\":\"1024\"," + "\"" + MRJobConfig.IO_SORT_FACTOR + "\":\"10\"}";

    CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);

    CloseableHttpResponse httpResponse = Mockito.mock(CloseableHttpResponse.class);
    StringEntity stringEntity = new StringEntity(expectedJSON);
    StatusLine statusLine = Mockito.mock(StatusLine.class);

    Mockito.when(statusLine.getStatusCode()).thenReturn(200);
    Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
    Mockito.when(httpResponse.getEntity()).thenReturn(stringEntity);

    Mockito.when(httpClient.execute(Matchers.any(HttpPost.class))).thenReturn((CloseableHttpResponse) httpResponse);

    Props props = new Props();
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB, "100");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, "0.9");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB, "2048");
    props.put(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB, "2048");
    props.put(TuningCommonConstants.TUNING_API_END_POINT, "dummy_api_end_point");

    Props passingProps = new Props(null, props);
    TuningParameterUtils tuningParameterUtils = new TuningParameterUtils(httpClient);
    tuningParameterUtils.updateAutoTuningParameters(passingProps);

    Assert.assertEquals(MRJobConfig.IO_SORT_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB), "300");
    Assert.assertEquals(MRJobConfig.REDUCE_MEMORY_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB), "1024");
    Assert.assertEquals(MRJobConfig.MAP_MEMORY_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB), "1024");
    Assert.assertEquals(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT),
        "0.9");
    Assert.assertEquals(MRJobConfig.IO_SORT_FACTOR + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_FACTOR), "10");

    passingProps = new Props(null, props);
    Mockito.when(statusLine.getStatusCode()).thenReturn(300);
    tuningParameterUtils.updateAutoTuningParameters(passingProps);

    Assert.assertEquals(MRJobConfig.IO_SORT_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB), "100");
    Assert.assertEquals(MRJobConfig.REDUCE_MEMORY_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB), "2048");
    Assert.assertEquals(MRJobConfig.MAP_MEMORY_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB), "2048");
    Assert.assertEquals(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT),
        "0.9");

    passingProps = new Props(null, props);
    Mockito.when(statusLine.getStatusCode()).thenReturn(404);
    tuningParameterUtils.updateAutoTuningParameters(passingProps);

    Assert.assertEquals(MRJobConfig.IO_SORT_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.IO_SORT_MB), "100");
    Assert.assertEquals(MRJobConfig.REDUCE_MEMORY_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.REDUCE_MEMORY_MB), "2048");
    Assert.assertEquals(MRJobConfig.MAP_MEMORY_MB + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.MAP_MEMORY_MB), "2048");
    Assert.assertEquals(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT + " value not correct",
        passingProps.getString(HadoopConfigurationInjector.INJECT_PREFIX + MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT),
        "0.9");
  }
}
