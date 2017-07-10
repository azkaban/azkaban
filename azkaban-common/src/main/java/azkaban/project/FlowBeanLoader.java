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

import azkaban.utils.Props;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.yaml.snakeyaml.Yaml;

public class FlowBeanLoader {

  public FlowBean load(final File flowFile) throws FileNotFoundException {
    checkArgument(flowFile.exists());
    checkArgument(flowFile.getName().endsWith(".yml"));

    return new Yaml().loadAs(new FileInputStream(flowFile), FlowBean.class);
  }

  public boolean validate(final FlowBean flowBean) {
    final Set<String> nodeNames = new HashSet<>();
    for (final NodeBean n : flowBean.getNodes()) {
      if (!nodeNames.add(n.getName())) {
        // Duplicate jobs
        return false;
      }
    }

    for (final NodeBean n : flowBean.getNodes()) {
      if (!nodeNames.containsAll(n.getDependsOn())) {
        // Undefined reference to dependent job
        return false;
      }
    }

    return true;
  }

  public AzkabanFlow toAzkabanFlow(final String flowName, final FlowBean flowBean) {
    final AzkabanFlow flow = new AzkabanFlow.AzkabanFlowBuilder()
        .setName(flowName)
        .setProps(new Props(null, flowBean.getConfig()))
        .setNodes(
            flowBean.getNodes().stream().map(this::toAzkabanNode).collect(Collectors.toList()))
        .build();
    return flow;
  }

  private AzkabanNode toAzkabanNode(final NodeBean nodeBean) {
    // Note: For now, all DAG nodes are assumed to be Jobs. The AzkabanNode generalize is for
    // future so that flows can refer to flows within it.

    return new AzkabanJob.AzkabanJobBuilder()
        .setName(nodeBean.getName())
        .setProps(new Props(null, nodeBean.getConfig()))
        .setType(nodeBean.getType())
        .setDependsOn(nodeBean.getDependsOn())
        .build();
  }

  public String getFlowName(final File flowFile) {
    checkArgument(flowFile.exists());
    checkArgument(flowFile.getName().endsWith(".yml"));

    return Files.getNameWithoutExtension(flowFile.getName());
  }
}
