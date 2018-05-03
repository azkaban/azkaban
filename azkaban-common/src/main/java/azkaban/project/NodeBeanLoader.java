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

import static com.google.common.base.Preconditions.checkArgument;

import azkaban.Constants;
import azkaban.Constants.FlowTriggerProps;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.quartz.CronExpression;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads NodeBean from YAML files.
 */
public class NodeBeanLoader {

  public NodeBean load(final File flowFile) throws Exception {
    checkArgument(flowFile != null && flowFile.exists());
    checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

    final NodeBean nodeBean = new Yaml().loadAs(new FileInputStream(flowFile), NodeBean.class);
    if (nodeBean == null) {
      throw new ProjectManagerException(
          "Failed to load flow file " + flowFile.getName() + ". Node bean is null .");
    }
    nodeBean.setName(getFlowName(flowFile));
    nodeBean.setType(Constants.FLOW_NODE_TYPE);
    return nodeBean;
  }

  public boolean validate(final NodeBean nodeBean) {
    final Set<String> nodeNames = new HashSet<>();
    for (final NodeBean n : nodeBean.getNodes()) {
      if (!nodeNames.add(n.getName())) {
        // Duplicate jobs
        return false;
      }
    }

    for (final NodeBean n : nodeBean.getNodes()) {
      if (n.getDependsOn() != null && !nodeNames.containsAll(n.getDependsOn())) {
        // Undefined reference to dependent job
        return false;
      }
    }

    return true;
  }

  public AzkabanNode toAzkabanNode(final NodeBean nodeBean) {
    if (nodeBean.getType().equals(Constants.FLOW_NODE_TYPE)) {
      return new AzkabanFlow.AzkabanFlowBuilder()
          .name(nodeBean.getName())
          .props(nodeBean.getProps())
          .dependsOn(nodeBean.getDependsOn())
          .nodes(nodeBean.getNodes().stream().map(this::toAzkabanNode).collect(Collectors.toList()))
          .flowTrigger(toFlowTrigger(nodeBean.getTrigger()))
          .build();
    } else {
      return new AzkabanJob.AzkabanJobBuilder()
          .name(nodeBean.getName())
          .props(nodeBean.getProps())
          .type(nodeBean.getType())
          .dependsOn(nodeBean.getDependsOn())
          .build();
    }
  }

  private void validateSchedule(final FlowTriggerBean flowTriggerBean) {
    final Map<String, String> scheduleMap = flowTriggerBean.getSchedule();

    Preconditions.checkNotNull(scheduleMap, "flow trigger schedule must not be null");

    Preconditions.checkArgument(
        scheduleMap.containsKey(FlowTriggerProps.SCHEDULE_TYPE) && scheduleMap.get
            (FlowTriggerProps.SCHEDULE_TYPE)
            .equals(FlowTriggerProps.CRON_SCHEDULE_TYPE),
        "flow trigger schedule type must be cron");

    Preconditions
        .checkArgument(scheduleMap.containsKey(FlowTriggerProps.SCHEDULE_VALUE) && CronExpression
                .isValidExpression(scheduleMap.get(FlowTriggerProps.SCHEDULE_VALUE)),
            "flow trigger schedule value must be a valid cron expression");

    final String cronExpression = scheduleMap.get(FlowTriggerProps.SCHEDULE_VALUE).trim();
    final String[] cronParts = cronExpression.split("\\s+");

//    Preconditions
//        .checkArgument(cronParts[0].equals("0"), "interval of flow trigger schedule has to"
//            + " be larger than 1 min");

    Preconditions.checkArgument(scheduleMap.size() == 2, "flow trigger schedule must "
        + "contain type and value only");
  }

  private void validateFlowTriggerBean(final FlowTriggerBean flowTriggerBean) {
    // validate max wait mins
    Preconditions.checkArgument(flowTriggerBean.getMaxWaitMins() >= Constants
        .MIN_FLOW_TRIGGER_WAIT_TIME.toMinutes(), "max wait min must be at least " + Constants
        .MIN_FLOW_TRIGGER_WAIT_TIME.toMinutes() + " min(s)");

    validateSchedule(flowTriggerBean);
    validateTriggerDependencies(flowTriggerBean.getTriggerDependencies());
  }

  /**
   * check uniqueness of dependency.name
   */
  private void validateDepNameUniqueness(final List<TriggerDependencyBean> dependencies) {
    final Set<String> seen = new HashSet<>();
    for (final TriggerDependencyBean dep : dependencies) {
      // set.add() returns false when there exists duplicate
      Preconditions.checkArgument(seen.add(dep.getName()), String.format("duplicate dependency"
          + ".name %s found, dependency.name should be unique", dep.getName()));
    }
  }

  /**
   * check uniqueness of dependency type and params
   */
  private void validateDepDefinitionUniqueness(final List<TriggerDependencyBean> dependencies) {
    for (int i = 0; i < dependencies.size(); i++) {
      for (int j = i + 1; j < dependencies.size(); j++) {
        final boolean duplicateDepDefFound =
            dependencies.get(i).getType().equals(dependencies.get(j)
                .getType()) && dependencies.get(i).getParams()
                .equals(dependencies.get(j).getParams());
        Preconditions.checkArgument(!duplicateDepDefFound, String.format("duplicate dependency"
                + "config %s found, dependency config should be unique",
            dependencies.get(i).getName()));
      }
    }
  }

  /**
   * validate name and type are present
   */
  private void validateNameAndTypeArePresent(final List<TriggerDependencyBean> dependencies) {
    for (final TriggerDependencyBean dep : dependencies) {
      Preconditions.checkNotNull(dep.getName(), "dependency name is required");
      Preconditions.checkNotNull(dep.getType(), "dependency type is required for " + dep.getName());
    }
  }

  private void validateTriggerDependencies(final List<TriggerDependencyBean> dependencies) {
    validateNameAndTypeArePresent(dependencies);
    validateDepNameUniqueness(dependencies);
    validateDepDefinitionUniqueness(dependencies);
    validateDepType(dependencies);
  }

  private void validateDepType(final List<TriggerDependencyBean> dependencies) {
    //todo chengren311: validate dependencies are of valid dependency type
  }

  public FlowTrigger toFlowTrigger(final FlowTriggerBean flowTriggerBean) {
    if (flowTriggerBean == null) {
      return null;
    } else {
      validateFlowTriggerBean(flowTriggerBean);
      if (flowTriggerBean.getMaxWaitMins() > Constants.DEFAULT_FLOW_TRIGGER_MAX_WAIT_TIME
          .toMinutes()) {
        flowTriggerBean.setMaxWaitMins(Constants.DEFAULT_FLOW_TRIGGER_MAX_WAIT_TIME.toMinutes());
      }
      return new FlowTrigger(
          new CronSchedule(flowTriggerBean.getSchedule().get(FlowTriggerProps.SCHEDULE_VALUE)),
          flowTriggerBean.getTriggerDependencies().stream()
              .map(d -> new FlowTriggerDependency(d.getName(), d.getType(), d.getParams()))
              .collect(Collectors.toList()),
          Duration.ofMinutes(flowTriggerBean.getMaxWaitMins()));
    }
  }

  public String getFlowName(final File flowFile) {
    checkArgument(flowFile != null && flowFile.exists());
    checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

    return Files.getNameWithoutExtension(flowFile.getName());
  }
}
