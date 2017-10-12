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
import azkaban.utils.Props;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads NodeBean from YAML files.
 */
public class NodeBeanLoader {

  private static final String NODE_BEAN_TYPE_FLOW = "flow";

  public NodeBean load(final File flowFile) throws FileNotFoundException {
    checkArgument(flowFile.exists());
    checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

    final NodeBean nodeBean = new Yaml().loadAs(new FileInputStream(flowFile), NodeBean.class);
    nodeBean.setName(getFlowName(flowFile));
    nodeBean.setType(NODE_BEAN_TYPE_FLOW);
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
      if (!nodeNames.containsAll(n.getDependsOn())) {
        // Undefined reference to dependent job
        return false;
      }
    }

    return true;
  }

  public AzkabanNode toAzkabanNode(final NodeBean nodeBean) {
    if (nodeBean.getType().equals(NODE_BEAN_TYPE_FLOW)) {
      return new AzkabanFlow.AzkabanFlowBuilder()
          .setName(nodeBean.getName())
          .setProps(new Props(null, nodeBean.getConfig()))
          .setDependsOn(nodeBean.getDependsOn())
          .setNodes(
              nodeBean.getNodes().stream().map(this::toAzkabanNode).collect(Collectors.toList()))
          .build();
    } else {
      return new AzkabanJob.AzkabanJobBuilder()
          .setName(nodeBean.getName())
          .setProps(new Props(null, nodeBean.getConfig()))
          .setType(nodeBean.getType())
          .setDependsOn(nodeBean.getDependsOn())
          .build();
    }
  }

  public String getFlowName(final File flowFile) {
    checkArgument(flowFile.exists());
    checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

    return Files.getNameWithoutExtension(flowFile.getName());
  }
}
