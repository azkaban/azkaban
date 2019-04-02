/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.sla;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.sla.SlaOption.SlaOptionBuilder;
import azkaban.utils.JSONUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

/** Test SlaOption */
public class SlaOptionTest {

  @Test
  public void testSlaOptionCreation() {
    final String flow = "flow";
    final String job = "job";
    final Set<SlaAction> actions = Collections.singleton(SlaAction.ALERT);
    final List<String> emails = Collections.singletonList("test@email.com");
    final Duration duration = Duration.ofHours(1);
    // negative test: null flow
    assertThatThrownBy(() -> new SlaOption(SlaType.JOB_FINISH, null, job, duration,
        actions, emails)).isInstanceOf(NullPointerException.class);

    // negative test: null type
    assertThatThrownBy(() -> new SlaOption(null, flow, job, duration,
        actions, emails)).isInstanceOf(NullPointerException.class);

    // negative test: null action
    assertThatThrownBy(() -> new SlaOption(SlaType.JOB_FINISH, flow, job, duration,
        null, emails)).isInstanceOf(NullPointerException.class);

    // negative test: null duration
    assertThatThrownBy(() -> new SlaOption(SlaType.JOB_FINISH, flow, job, null,
        actions, emails)).isInstanceOf(NullPointerException.class);

    // negative test: empty action
    assertThatThrownBy(() -> new SlaOption(SlaType.JOB_FINISH, flow, job, duration,
        Collections.emptySet(), emails)).isInstanceOf(IllegalStateException.class);

    SlaOption slaOption = new SlaOption(SlaType.JOB_FINISH, flow, job, duration,
        actions, emails);
    assertThat(slaOption.getEmails()).isEqualTo(emails);
    assertThat(slaOption.getType()).isEqualTo(SlaType.JOB_FINISH);
    assertThat(slaOption.getDuration()).isEqualTo(duration);
    assertThat(slaOption.getFlowName()).isEqualTo(flow);
    assertThat(slaOption.getJobName()).isEqualTo(job);
    assertThat(slaOption.hasAlert()).isTrue();
    assertThat(slaOption.hasKill()).isFalse();
  }

  @Test
  public void testSlaActionFiltering() {
    List<SlaOption> slaOptions = new ArrayList<>();
    slaOptions.add(new SlaOptionBuilder(SlaType.FLOW_FINISH, "flow1", Duration.ofHours(1))
        .setActions(Collections.singleton(SlaAction.KILL)).createSlaOption());
    slaOptions.add(new SlaOptionBuilder(SlaType.FLOW_SUCCEED, "flow2", Duration.ofHours(1))
        .setActions(Collections.singleton(SlaAction.ALERT)).setEmails(Collections.singletonList
            ("test@email.com")).createSlaOption());
    slaOptions.add(new SlaOptionBuilder(SlaType.JOB_FINISH, "flow3", Duration.ofHours(1))
        .setActions(Collections.singleton(SlaAction.KILL)).setJobName("job").createSlaOption());
    slaOptions.add(new SlaOptionBuilder(SlaType.JOB_SUCCEED, "flow4", Duration.ofHours(1))
        .setActions(Collections.singleton(SlaAction.KILL)).setJobName("job").createSlaOption());

    List<SlaOption> flowOptions = SlaOption.getFlowLevelSLAOptions(slaOptions);
    assertThat(flowOptions.size()).isEqualTo(2);
    Set<String> flowNames = flowOptions.stream().map(x -> x.getFlowName()).collect(Collectors.toSet
        ());
    assertThat(flowNames.containsAll(Sets.newHashSet("flow1", "flow2"))).isTrue();

    List<SlaOption> jobOptions = SlaOption.getJobLevelSLAOptions(slaOptions);
    assertThat(flowOptions.size()).isEqualTo(2);
    Set<String> jobFlowNames = jobOptions.stream().map(x -> x.getFlowName()).collect(Collectors
        .toSet
        ());
    assertThat(jobFlowNames.containsAll(Sets.newHashSet("flow3", "flow4"))).isTrue();
  }

  @Test
  public void testWebObject() {
    final String expectedDuration = "60m";
    final String expectedStatus = "FINISH";
    final String expectedId = "job";
    final List<String> expectedAction = Collections.singletonList(SlaOption.WEB_ACTION_KILL);
    SlaOption slaOption = new SlaOptionBuilder(SlaType.JOB_FINISH, "flow", Duration.ofHours(1))
        .setJobName("job").setActions(Collections.singleton(SlaAction.KILL))
        .setEmails(Collections.singletonList("test@email.com")).createSlaOption();

    Map<String, Object> webObject = (Map<String, Object>)slaOption.toWebObject();
    assertThat((String)webObject.get(SlaOption.WEB_DURATION)).isEqualTo(expectedDuration);
    assertThat((String)webObject.get(SlaOption.WEB_STATUS)).isEqualTo(expectedStatus);
    assertThat((String)webObject.get(SlaOption.WEB_ID)).isEqualTo(expectedId);
    assertThat((List<String>)webObject.get(SlaOption.WEB_ACTIONS)).isEqualTo(expectedAction);
  }

  @Test
  public void testSerialization() {
    List<String> emails = Lists.newArrayList("test@email.com", "user@email.com");
    SlaOption slaOption = new SlaOptionBuilder(SlaType.JOB_FINISH, "flow", Duration.ofHours(1))
        .setJobName("job").setActions(Sets.newHashSet(SlaAction.KILL, SlaAction.ALERT))
        .setEmails(emails).createSlaOption();
    Object slaObject = slaOption.toObject();

    List<String> deprecatedActions = Lists.newArrayList(SlaOptionDeprecated.ACTION_ALERT,
        SlaOptionDeprecated.ACTION_KILL_JOB);
    Map<String, Object> deprecatedInfo = new HashMap<>();
    deprecatedInfo.put(SlaOptionDeprecated.INFO_FLOW_NAME, "flow");
    deprecatedInfo.put(SlaOptionDeprecated.ALERT_TYPE, SlaOption.ALERT_TYPE_EMAIL);
    deprecatedInfo.put(SlaOptionDeprecated.INFO_JOB_NAME, "job");
    deprecatedInfo.put(SlaOptionDeprecated.INFO_DURATION, "60m");
    deprecatedInfo.put(SlaOptionDeprecated.INFO_EMAIL_LIST, emails);

    SlaOptionDeprecated slaOptionDeprecated = new SlaOptionDeprecated(SlaOptionDeprecated
        .TYPE_JOB_FINISH, deprecatedActions, deprecatedInfo);

    assertThat(slaObject).isEqualTo(slaOptionDeprecated.toObject());
    SlaOption slaOption2 = SlaOption.fromObject(slaObject);

    compare(slaOption, slaOption2);
  }

  /** Compare if two {@link SlaOption} are the same */
  private void compare(SlaOption option1, SlaOption option2) {
    assertThat(option1.getType()).isEqualTo(option2.getType());
    assertThat(option1.getDuration()).isEqualTo(option2.getDuration());
    assertThat(option1.getFlowName()).isEqualTo(option2.getFlowName());
    assertThat(option1.getJobName()).isEqualTo(option2.getJobName());
    assertThat(option1.hasAlert()).isEqualTo(option2.hasAlert());
    assertThat(option1.hasKill()).isEqualTo(option2.hasKill());
    assertThat(option1.getEmails()).isEqualTo(option2.getEmails());
  }
}

