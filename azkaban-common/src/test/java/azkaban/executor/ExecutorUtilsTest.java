/*
 * Copyright 2019 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the “License”); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package azkaban.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

/** Tests for {@link ExecutorUtils}. */
public class ExecutorUtilsTest {
  @Test
  public void testGetMaxConcurrentRunsOneFlow() {
    Props props = new Props();
    assertThat(ExecutorUtils.getMaxConcurrentRunsOneFlow(props)).isEqualTo(
        Constants.DEFAULT_MAX_ONCURRENT_RUNS_ONEFLOW);

    // set explictly
    props.put(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW, 5);
    assertThat(ExecutorUtils.getMaxConcurrentRunsOneFlow(props)).isEqualTo(
        5);
  }

  @Test
  public void testGetMaxConcurentRunsPerFlowMap() {
    Props props = new Props();

    // empty map is returned if whitelist is not set
    assertThat(ExecutorUtils.getMaxConcurentRunsPerFlowMap(props).size()).isEqualTo(0);

    props.put(ConfigurationKeys.CONCURRENT_RUNS_ONEFLOW_WHITELIST,
        "project1,flow1,300;project2,flow2,5");

    Map<Pair<String, String>, Integer> map =  ExecutorUtils.getMaxConcurentRunsPerFlowMap(props);
    assertThat(map.size()).isEqualTo(2);
    assertThat(map.get(new Pair("project1", "flow1"))).isEqualTo(300);
    assertThat(map.get(new Pair("project2", "flow2"))).isEqualTo(5);

    // negative test: missing flow
    props.put(ConfigurationKeys.CONCURRENT_RUNS_ONEFLOW_WHITELIST,
        "project1,flow1,300;project2,5");
    assertThatThrownBy(() -> ExecutorUtils.getMaxConcurentRunsPerFlowMap(props)).isInstanceOf
        (IllegalStateException.class);
  }

  @Test
  public void testGetMaxConcurrentRunsForFlow() {
    Props props = new Props();
    props.put(Constants.ConfigurationKeys.MAX_CONCURRENT_RUNS_ONEFLOW, 10);
    props.put(ConfigurationKeys.CONCURRENT_RUNS_ONEFLOW_WHITELIST,
        "project1,flow1,300;project2,flow2,5");
    Map<Pair<String, String>, Integer> map =  ExecutorUtils.getMaxConcurentRunsPerFlowMap(props);
    int defaultMaxConcurrent = ExecutorUtils.getMaxConcurrentRunsOneFlow(props);
    assertThat(ExecutorUtils.getMaxConcurrentRunsForFlow("project1", "flow1",
        defaultMaxConcurrent, map)).isEqualTo(300);
    assertThat(ExecutorUtils.getMaxConcurrentRunsForFlow("project2", "flow2",
        defaultMaxConcurrent, map)).isEqualTo(5);
    assertThat(ExecutorUtils.getMaxConcurrentRunsForFlow("project3", "flow3",
        defaultMaxConcurrent, map)).isEqualTo(10);

    // return default value if there are no if the whitelisted flow map is empty
    assertThat(ExecutorUtils.getMaxConcurrentRunsForFlow("project1", "flow1",
        defaultMaxConcurrent, Collections.EMPTY_MAP)).isEqualTo(10);
  }
}
