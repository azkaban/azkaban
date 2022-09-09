/*
 * Copyright 2022 LinkedIn Corp.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;
import org.junit.Test;

public class YarnUtilsTest {

  final private Logger log = Logger.getLogger(YarnUtilsTest.class);

  @Test
  public void testKillAppOnCluster() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doNothing().when(mockClient).killApplication(any());
    YarnUtils.killAppOnCluster(mockClient, "application_123_456", log);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testKillAppOnClusterInvalidAppID() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doNothing().when(mockClient).killApplication(any());
    YarnUtils.killAppOnCluster(mockClient, "application+++123===456", log);
  }

  @Test(expected = YarnException.class)
  public void testKillAppOnClusterYarnFail() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doThrow(new YarnException("ops")).when(mockClient).killApplication(any());
    YarnUtils.killAppOnCluster(mockClient, "application_123_456", log);
  }

  @Test
  public void testKillAllAppsOnCluster() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doNothing().when(mockClient).killApplication(any());
    YarnUtils.killAllAppsOnCluster(mockClient,
        ImmutableSet.of("application_123_456", "application_456_123"), log);
  }

  @Test
  public void testKillAllAppsOnClusterFail() throws IOException, YarnException {
    YarnClient mockClient = mock(YarnClient.class);
    doNothing().when(mockClient).killApplication(any());
    doThrow(new YarnException("ops")).when(mockClient).killApplication(
        eq(ApplicationId.newInstance(4560, 1230)));

    // log the error but not throw
    YarnUtils.killAllAppsOnCluster(mockClient,
        ImmutableSet.of("application_123_456", "application_4560_1230"), log);
  }
}
