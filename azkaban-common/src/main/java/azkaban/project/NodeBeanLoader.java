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
import com.google.common.io.Files;
import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
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

  public FlowTrigger toFlowTrigger(final FlowTriggerBean flowTriggerBean) {
    // Todo jamiesjc: need to validate flowTriggerBean
    return flowTriggerBean == null ? null
        : new FlowTrigger(
            new CronSchedule(flowTriggerBean.getSchedule().get(Constants.SCHEDULE_VALUE)),
            flowTriggerBean.getTriggerDependencies().stream()
                .map(d -> new FlowTriggerDependency(d.getName(), d.getType(), d.getParams()))
                .collect(Collectors.toList()),
            Duration.ofMinutes(flowTriggerBean.getMaxWaitMins()));
  }

  public String getFlowName(final File flowFile) {
    checkArgument(flowFile != null && flowFile.exists());
    checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

    return Files.getNameWithoutExtension(flowFile.getName());
  }
}
