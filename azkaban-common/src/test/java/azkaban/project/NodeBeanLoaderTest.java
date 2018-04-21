/*
 * Copyright 2017 LinkedIn Corp.
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
 *
 */

package azkaban.project;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.Constants;
import azkaban.Constants.FlowTriggerProps;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.utils.Props;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class NodeBeanLoaderTest {

  private static final String BASIC_FLOW_YML_TEST_DIR = "basicflowyamltest";
  private static final String BASIC_FLOW_NAME = "basic_flow";
  private static final String BASIC_FLOW_YML_FILE = BASIC_FLOW_NAME + ".flow";
  private static final String EMBEDDED_FLOW_YML_TEST_DIR = "embeddedflowyamltest";
  private static final String EMBEDDED_FLOW_NAME = "embedded_flow";
  private static final String EMBEDDED_FLOW_YML_FILE = EMBEDDED_FLOW_NAME + ".flow";
  private static final String TRIGGER_FLOW_YML_TEST_DIR = "flowtriggeryamltest";
  private static final String TRIGGER_FLOW_NAME = "flow_trigger";
  private static final String TRIGGER_FLOW_YML_FILE = TRIGGER_FLOW_NAME + ".flow";
  private static final String FLOW_CONFIG_KEY = "flow-level-parameter";
  private static final String FLOW_CONFIG_VALUE = "value";
  private static final String SHELL_END = "shell_end";
  private static final String SHELL_ECHO = "shell_echo";
  private static final String SHELL_BASH = "shell_bash";
  private static final String SHELL_PWD = "shell_pwd";
  private static final String ECHO_COMMAND = "echo \"This is an echoed text.\"";
  private static final String ECHO_COMMAND_1 = "echo \"This is an echoed text from embedded_flow1.\"";
  private static final String ECHO_OVERRIDE = "echo \"Override job properties.\"";
  private static final String PWD_COMMAND = "pwd";
  private static final String BASH_COMMAND = "bash ./sample_script.sh";
  private static final String EMBEDDED_FLOW1 = "embedded_flow1";
  private static final String EMBEDDED_FLOW2 = "embedded_flow2";
  private static final String TYPE_NOOP = "noop";
  private static final String TYPE_COMMAND = "command";
  private static final int MAX_WAIT_MINS = 5;
  private static final String CRON_EXPRESSION = "0 0 1 ? * *";
  private static final String TRIGGER_NAME_1 = "search-impression";
  private static final String TRIGGER_NAME_2 = "other-name";
  private static final String TRIGGER_TYPE = "dali-dataset";
  private static final ImmutableMap<String, String> PARAMS_1 = ImmutableMap
      .of("view", "search_mp_versioned.search_impression_event_0_0_47", "delay", "1", "window", "1",
          "unit", "daily", "filter", "is_guest=0");
  private static final ImmutableMap<String, String> PARAMS_2 = ImmutableMap
      .of("view", "another dataset",
          "delay", "1", "window", "7");

  @Test
  public void testLoadNodeBeanForBasicFlow() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));

    validateNodeBean(nodeBean, BASIC_FLOW_NAME, Constants.FLOW_NODE_TYPE, FLOW_CONFIG_KEY,
        FLOW_CONFIG_VALUE, 4, null);
    validateNodeBean(nodeBean.getNodes().get(0), SHELL_END, TYPE_NOOP, null,
        null, 0, Arrays.asList(SHELL_PWD, SHELL_ECHO, SHELL_BASH));
    validateNodeBean(nodeBean.getNodes().get(1), SHELL_ECHO, TYPE_COMMAND, TYPE_COMMAND,
        ECHO_COMMAND, 0, null);
  }

  @Test
  public void testLoadNodeBeanForEmbeddedFlow() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));

    validateNodeBean(nodeBean, EMBEDDED_FLOW_NAME, Constants.FLOW_NODE_TYPE, FLOW_CONFIG_KEY,
        FLOW_CONFIG_VALUE, 4, null);
    validateNodeBean(nodeBean.getNodes().get(0), SHELL_END, TYPE_NOOP, null,
        null, 0, Arrays.asList(SHELL_PWD, SHELL_ECHO, EMBEDDED_FLOW1));
    validateNodeBean(nodeBean.getNodes().get(1), SHELL_PWD, TYPE_COMMAND, TYPE_COMMAND,
        PWD_COMMAND, 0, null);
    validateNodeBean(nodeBean.getNodes().get(2), SHELL_ECHO, TYPE_COMMAND, TYPE_COMMAND,
        ECHO_COMMAND, 0, null);
    validateNodeBean(nodeBean.getNodes().get(3), EMBEDDED_FLOW1, Constants.FLOW_NODE_TYPE,
        FLOW_CONFIG_KEY, FLOW_CONFIG_VALUE, 4, null);

    // Verify nodes in embedded_flow1 are loaded correctly.
    final NodeBean embeddedNodeBean1 = nodeBean.getNodes().get(3);
    validateNodeBean(embeddedNodeBean1.getNodes().get(0), SHELL_END, TYPE_NOOP, null, null, 0,
        Arrays.asList(SHELL_ECHO, EMBEDDED_FLOW2));
    validateNodeBean(embeddedNodeBean1.getNodes().get(1), SHELL_ECHO, TYPE_COMMAND, TYPE_COMMAND,
        ECHO_COMMAND_1, 0, null);
    validateNodeBean(embeddedNodeBean1.getNodes().get(2), EMBEDDED_FLOW2, Constants.FLOW_NODE_TYPE,
        FLOW_CONFIG_KEY, FLOW_CONFIG_VALUE, 2, Arrays.asList(SHELL_BASH));
    validateNodeBean(embeddedNodeBean1.getNodes().get(3), SHELL_BASH, TYPE_COMMAND, TYPE_COMMAND,
        BASH_COMMAND, 0, null);

    // Verify nodes in embedded_flow2 are loaded correctly.
    validateNodeBean(embeddedNodeBean1.getNodes().get(2).getNodes().get(0), SHELL_END, TYPE_NOOP,
        null,
        null, 0, Arrays.asList(SHELL_PWD));
    validateNodeBean(embeddedNodeBean1.getNodes().get(2).getNodes().get(1), SHELL_PWD, TYPE_COMMAND,
        TYPE_COMMAND, PWD_COMMAND, 0, null);
  }

  @Test
  public void testLoadNodeBeanForFlowTrigger() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, TRIGGER_FLOW_YML_FILE));
    final Map<String, String> schedule = ImmutableMap
        .of(FlowTriggerProps.SCHEDULE_TYPE, FlowTriggerProps
            .CRON_SCHEDULE_TYPE, FlowTriggerProps.SCHEDULE_VALUE, CRON_EXPRESSION);
    validateFlowTriggerBean(nodeBean.getTrigger(), MAX_WAIT_MINS, schedule, 2);
    final List<TriggerDependencyBean> triggerDependencyBeans = nodeBean.getTrigger()
        .getTriggerDependencies();
    validateTriggerDependencyBean(triggerDependencyBeans.get(0), TRIGGER_NAME_1, TRIGGER_TYPE,
        PARAMS_1);
    validateTriggerDependencyBean(triggerDependencyBeans.get(1), TRIGGER_NAME_2, TRIGGER_TYPE,
        PARAMS_2);
  }

  @Test
  public void testToBasicAzkabanFlow() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));
    final AzkabanFlow flow = (AzkabanFlow) loader.toAzkabanNode(nodeBean);

    final Props props = new Props();
    props.put(Constants.NODE_TYPE, Constants.FLOW_NODE_TYPE);
    props.put(FLOW_CONFIG_KEY, FLOW_CONFIG_VALUE);
    validateAzkabanNode(flow, BASIC_FLOW_NAME, Constants.FLOW_NODE_TYPE, props, Arrays.asList
        (SHELL_END, SHELL_PWD, SHELL_ECHO, SHELL_BASH), null);

    final Props props1 = new Props();
    props1.put(Constants.NODE_TYPE, TYPE_NOOP);
    validateAzkabanNode(flow.getNode(SHELL_END), SHELL_END, TYPE_NOOP, props1, null,
        Arrays.asList(SHELL_PWD, SHELL_ECHO, SHELL_BASH));

    final Props props2 = new Props();
    props2.put(Constants.NODE_TYPE, TYPE_COMMAND);
    props2.put(TYPE_COMMAND, ECHO_COMMAND);
    validateAzkabanNode(flow.getNode(SHELL_ECHO), SHELL_ECHO, TYPE_COMMAND, props2, null,
        null);
  }

  @Test
  public void testToEmbeddedAzkabanFlow() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));
    final AzkabanFlow flow = (AzkabanFlow) loader.toAzkabanNode(nodeBean);

    final Props props = new Props();
    props.put(Constants.NODE_TYPE, Constants.FLOW_NODE_TYPE);
    props.put(FLOW_CONFIG_KEY, FLOW_CONFIG_VALUE);
    validateAzkabanNode(flow, EMBEDDED_FLOW_NAME, Constants.FLOW_NODE_TYPE, props, Arrays.asList
        (SHELL_END, SHELL_PWD, SHELL_ECHO, EMBEDDED_FLOW1), null);

    final Props props1 = new Props();
    props1.put(Constants.NODE_TYPE, TYPE_NOOP);
    validateAzkabanNode(flow.getNode(SHELL_END), SHELL_END, TYPE_NOOP, props1, null,
        Arrays.asList(SHELL_PWD, SHELL_ECHO, EMBEDDED_FLOW1));

    final Props props2 = new Props();
    props2.put(Constants.NODE_TYPE, Constants.FLOW_NODE_TYPE);
    props2.put(FLOW_CONFIG_KEY, FLOW_CONFIG_VALUE);
    final AzkabanFlow embeddedFlow1 = (AzkabanFlow) flow.getNode(EMBEDDED_FLOW1);
    validateAzkabanNode(embeddedFlow1, EMBEDDED_FLOW1, Constants.FLOW_NODE_TYPE, props2,
        Arrays.asList
            (SHELL_END, SHELL_BASH, SHELL_ECHO, EMBEDDED_FLOW2), null);

    final AzkabanFlow embeddedFlow2 = (AzkabanFlow) embeddedFlow1.getNode(EMBEDDED_FLOW2);
    validateAzkabanNode(embeddedFlow2, EMBEDDED_FLOW2, Constants.FLOW_NODE_TYPE,
        props2, Arrays.asList(SHELL_END, SHELL_PWD), Arrays.asList(SHELL_BASH));
  }

  @Test
  public void testToFlowTrigger() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, TRIGGER_FLOW_YML_FILE));
    final FlowTrigger flowTrigger = loader.toFlowTrigger(nodeBean.getTrigger());
    validateFlowTrigger(flowTrigger, MAX_WAIT_MINS, CRON_EXPRESSION, 2);
    validateTriggerDependency(flowTrigger, TRIGGER_NAME_1, TRIGGER_TYPE, PARAMS_1);
    validateTriggerDependency(flowTrigger, TRIGGER_NAME_2, TRIGGER_TYPE, PARAMS_2);
  }

  @Test
  public void testFlowTriggerMaxWaitMinValidation() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();

    NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_no_max_wait_min.flow"));
    FlowTrigger flowTrigger = loader.toFlowTrigger(nodeBean.getTrigger());
    assertThat(flowTrigger.getMaxWaitDuration())
        .isEqualTo(Constants.DEFAULT_FLOW_TRIGGER_MAX_WAIT_TIME);

    nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_large_max_wait_min.flow"));
    flowTrigger = loader.toFlowTrigger(nodeBean.getTrigger());
    assertThat(flowTrigger.getMaxWaitDuration())
        .isEqualTo(Constants.DEFAULT_FLOW_TRIGGER_MAX_WAIT_TIME);

    final NodeBean nodeBean2 = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_zero_max_wait_min.flow"));

    assertThatThrownBy(() -> loader.toFlowTrigger(nodeBean2.getTrigger()))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("max wait min must be at least 1"
        + " min(s)");
  }

  @Test
  public void testFlowTriggerDepDuplicationValidation() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();

    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_duplicate_dep_props.flow"));

    assertThatThrownBy(() -> loader.toFlowTrigger(nodeBean.getTrigger()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testFlowTriggerRequireDepNameAndType() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();

    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_without_dep_name.flow"));

    assertThatThrownBy(() -> loader.toFlowTrigger(nodeBean.getTrigger()))
        .isInstanceOf(NullPointerException.class);

    final NodeBean nodeBean2 = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_without_dep_type.flow"));

    assertThatThrownBy(() -> loader.toFlowTrigger(nodeBean2.getTrigger()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testFlowTriggerScheduleValidation() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();

    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_invalid_cron_expression.flow"));

    assertThatThrownBy(() -> loader.toFlowTrigger(nodeBean.getTrigger()))
        .isInstanceOf(IllegalArgumentException.class);

    loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_second_level_cron_expression1.flow"));

    assertThatThrownBy(() -> loader.toFlowTrigger(nodeBean.getTrigger()))
        .isInstanceOf(IllegalArgumentException.class);

    loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_second_level_cron_expression2.flow"));

    assertThatThrownBy(() -> loader.toFlowTrigger(nodeBean.getTrigger()))
        .isInstanceOf(IllegalArgumentException.class);

    final NodeBean nodeBean2 = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, "flow_trigger_no_schedule.flow"));

    assertThatThrownBy(() -> loader.toFlowTrigger(nodeBean2.getTrigger()))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testToAzkabanFlowWithFlowTrigger() throws Exception {
    final NodeBeanLoader loader = new NodeBeanLoader();
    final NodeBean nodeBean = loader.load(ExecutionsTestUtil.getFlowFile(
        TRIGGER_FLOW_YML_TEST_DIR, TRIGGER_FLOW_YML_FILE));
    final AzkabanFlow flow = (AzkabanFlow) loader.toAzkabanNode(nodeBean);
    validateFlowTrigger(flow.getFlowTrigger(), MAX_WAIT_MINS, CRON_EXPRESSION, 2);
    validateTriggerDependency(flow.getFlowTrigger(), TRIGGER_NAME_1,
        TRIGGER_TYPE,
        PARAMS_1);
    validateTriggerDependency(flow.getFlowTrigger(), TRIGGER_NAME_2,
        TRIGGER_TYPE,
        PARAMS_2);
  }

  @Test
  public void testGetFlowName() {
    assertThat(new NodeBeanLoader().getFlowName(ExecutionsTestUtil.getFlowFile(
        BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE))).isEqualTo(BASIC_FLOW_NAME);
  }

  @Test
  public void testGetFlowProps() {
    final Props flowProps = FlowLoaderUtils.getPropsFromYamlFile(BASIC_FLOW_NAME,
        ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));
    assertThat(flowProps.size()).isEqualTo(2);
    assertThat(flowProps.get(Constants.NODE_TYPE)).isEqualTo(Constants.FLOW_NODE_TYPE);
    assertThat(flowProps.get(FLOW_CONFIG_KEY)).isEqualTo(FLOW_CONFIG_VALUE);
  }

  @Test
  public void testGetJobPropsFromBasicFlow() {
    final Props jobProps = FlowLoaderUtils
        .getPropsFromYamlFile(BASIC_FLOW_NAME + Constants.PATH_DELIMITER + SHELL_ECHO,
            ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));
    assertThat(jobProps.size()).isEqualTo(2);
    assertThat(jobProps.get(Constants.NODE_TYPE)).isEqualTo(TYPE_COMMAND);
    assertThat(jobProps.get(TYPE_COMMAND)).isEqualTo(ECHO_COMMAND);
  }

  @Test
  public void testGetJobPropsWithInvalidPath() {
    final Props jobProps = FlowLoaderUtils
        .getPropsFromYamlFile(BASIC_FLOW_NAME + Constants.PATH_DELIMITER + EMBEDDED_FLOW_NAME,
            ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE));
    assertThat(jobProps).isNull();
  }

  @Test
  public void testGetJobPropsFromEmbeddedFlow() {
    // Get job props from parent flow
    String jobPrefix = EMBEDDED_FLOW_NAME + Constants.PATH_DELIMITER;
    Props jobProps = FlowLoaderUtils.getPropsFromYamlFile(jobPrefix + SHELL_ECHO,
        ExecutionsTestUtil.getFlowFile(EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));
    assertThat(jobProps.size()).isEqualTo(2);
    assertThat(jobProps.get(Constants.NODE_TYPE)).isEqualTo(TYPE_COMMAND);
    assertThat(jobProps.get(TYPE_COMMAND)).isEqualTo(ECHO_COMMAND);

    // Get job props from first level embedded flow
    jobPrefix = jobPrefix + EMBEDDED_FLOW1 + Constants.PATH_DELIMITER;
    jobProps = FlowLoaderUtils.getPropsFromYamlFile(jobPrefix + SHELL_ECHO,
        ExecutionsTestUtil.getFlowFile(EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));
    assertThat(jobProps.size()).isEqualTo(2);
    assertThat(jobProps.get(Constants.NODE_TYPE)).isEqualTo(TYPE_COMMAND);
    assertThat(jobProps.get(TYPE_COMMAND)).isEqualTo(ECHO_COMMAND_1);

    // Get job props from second level embedded flow
    jobPrefix = jobPrefix + EMBEDDED_FLOW2 + Constants.PATH_DELIMITER;
    jobProps = FlowLoaderUtils.getPropsFromYamlFile(jobPrefix + SHELL_PWD,
        ExecutionsTestUtil.getFlowFile(EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE));
    assertThat(jobProps.size()).isEqualTo(2);
    assertThat(jobProps.get(Constants.NODE_TYPE)).isEqualTo(TYPE_COMMAND);
    assertThat(jobProps.get(TYPE_COMMAND)).isEqualTo(PWD_COMMAND);
  }

  @Test
  public void testSetJobPropsInBasicFlow() throws Exception {
    final String path = BASIC_FLOW_NAME + Constants.PATH_DELIMITER + SHELL_ECHO;
    final Props overrideProps = new Props();
    overrideProps.put(Constants.NODE_TYPE, TYPE_COMMAND);
    overrideProps.put(TYPE_COMMAND, ECHO_OVERRIDE);

    final File newFile = new File(ExecutionsTestUtil.getDataRootDir(), BASIC_FLOW_YML_FILE);
    newFile.deleteOnExit();
    FileUtils.copyFile(ExecutionsTestUtil.getFlowFile(BASIC_FLOW_YML_TEST_DIR, BASIC_FLOW_YML_FILE),
        newFile);
    FlowLoaderUtils.setPropsInYamlFile(path, newFile, overrideProps);
    assertThat(FlowLoaderUtils.getPropsFromYamlFile(path, newFile)).isEqualTo(overrideProps);
  }

  @Test
  public void testSetJobPropsInEmbeddedFlow() throws Exception {
    final String path = EMBEDDED_FLOW_NAME + Constants.PATH_DELIMITER + EMBEDDED_FLOW1 +
        Constants.PATH_DELIMITER + EMBEDDED_FLOW2 + Constants.PATH_DELIMITER + SHELL_END;
    final Props overrideProps = new Props();
    overrideProps.put(Constants.NODE_TYPE, TYPE_COMMAND);
    overrideProps.put(TYPE_COMMAND, ECHO_OVERRIDE);

    final File newFile = new File(ExecutionsTestUtil.getDataRootDir(), EMBEDDED_FLOW_YML_FILE);
    newFile.deleteOnExit();
    FileUtils.copyFile(
        ExecutionsTestUtil.getFlowFile(EMBEDDED_FLOW_YML_TEST_DIR, EMBEDDED_FLOW_YML_FILE),
        newFile);
    FlowLoaderUtils.setPropsInYamlFile(path, newFile, overrideProps);
    assertThat(FlowLoaderUtils.getPropsFromYamlFile(path, newFile)).isEqualTo(overrideProps);
  }

  @Test
  public void testGetFlowTrigger() {
    final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(
        ExecutionsTestUtil.getFlowFile(TRIGGER_FLOW_YML_TEST_DIR, TRIGGER_FLOW_YML_FILE));
    validateFlowTrigger(flowTrigger, MAX_WAIT_MINS, CRON_EXPRESSION, 2);
    validateTriggerDependency(flowTrigger, TRIGGER_NAME_1, TRIGGER_TYPE,
        PARAMS_1);
    validateTriggerDependency(flowTrigger, TRIGGER_NAME_2, TRIGGER_TYPE,
        PARAMS_2);
  }

  private void validateNodeBean(final NodeBean nodeBean, final String name, final String type,
      final String configKey,
      final String configValue, final int numNodes, final List<String> dependsOn) {
    assertThat(nodeBean.getName()).isEqualTo(name);
    assertThat(nodeBean.getType()).isEqualTo(type);
    if (configKey != null) {
      assertThat(nodeBean.getConfig().get(configKey)).isEqualTo(configValue);
    }
    if (numNodes != 0) {
      assertThat(nodeBean.getNodes().size()).isEqualTo(numNodes);
    }
    if (dependsOn != null) {
      assertThat(nodeBean.getDependsOn()).containsOnlyElementsOf(dependsOn);
    }
  }

  private void validateAzkabanNode(final AzkabanNode azkabanNode, final String name,
      final String type, final Props props, final List<String> nodeList, final List<String>
      dependsOn) {
    assertThat(azkabanNode.getName()).isEqualTo(name);
    assertThat(azkabanNode.getType()).isEqualTo(type);
    assertThat(azkabanNode.getProps()).isEqualTo(props);
    if (azkabanNode instanceof AzkabanFlow) {
      assertThat(((AzkabanFlow) azkabanNode).getNodes().keySet()).containsOnlyElementsOf(nodeList);
    }
    if (dependsOn != null) {
      assertThat(azkabanNode.getDependsOn()).containsOnlyElementsOf(dependsOn);
    }
  }

  private void validateFlowTriggerBean(final FlowTriggerBean flowTriggerBean, final int
      maxWaitMins, final Map<String, String> schedule, final int numDependencies) {
    assertThat(flowTriggerBean.getMaxWaitMins()).isEqualTo(maxWaitMins);
    assertThat(flowTriggerBean.getSchedule()).isEqualTo(schedule);
    assertThat(flowTriggerBean.getTriggerDependencies().size()).isEqualTo(numDependencies);
  }

  private void validateTriggerDependencyBean(final TriggerDependencyBean triggerDependencyBean,
      final String name, final String type, final Map<String, String> params) {
    assertThat(triggerDependencyBean.getName()).isEqualTo(name);
    assertThat(triggerDependencyBean.getType()).isEqualTo(type);
    assertThat(triggerDependencyBean.getParams()).isEqualTo(params);
  }

  private void validateFlowTrigger(final FlowTrigger flowTrigger, final long maxWaitMins, final
  String cronExpression, final int numDependencies) {
    assertThat(flowTrigger.getMaxWaitDuration()).isEqualTo(Duration.ofMinutes(maxWaitMins));
    assertThat(flowTrigger.getSchedule().getCronExpression()).isEqualTo(cronExpression);
    assertThat(flowTrigger.getDependencies().size()).isEqualTo(numDependencies);
  }

  private void validateTriggerDependency(final FlowTrigger flowTrigger, final
  String name, final String type, final Map<String, String> params) {
    assertThat(flowTrigger.getDependencyByName(name)).isNotNull();
    assertThat(flowTrigger.getDependencyByName(name).getType()).isEqualTo(type);
    assertThat(flowTrigger.getDependencyByName(name).getProps()).isEqualTo(params);
  }
}

