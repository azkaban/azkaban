/*
 * Copyright 2015 LinkedIn Corp.
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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import azkaban.executor.ExecutableFlow;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.user.User;

/**
 * Commonly used utils method for unit/integration tests
 */
public class TestUtils {
  /* Directory with serialized description of test flows */
  private static final String UNIT_BASE_DIR =
    "../azkaban-test/src/test/resources/executions";

  public static File getFlowDir(String projectName, String flow) {
    return new File(String.format("%s/%s/%s.flow", UNIT_BASE_DIR, projectName,
      flow));
  }

  public static User getTestUser() {
    return new User("testUser");
  }

  /* Helper method to create an ExecutableFlow from serialized description */
  public static ExecutableFlow createExecutableFlow(String projectName, String flowName)
    throws IOException {
    File jsonFlowFile = getFlowDir(projectName, flowName);
    @SuppressWarnings("unchecked")
    HashMap<String, Object> flowObj =
      (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);

    Flow flow = Flow.flowFromObject(flowObj);
    Project project = new Project(1, "flow");
    HashMap<String, Flow> flowMap = new HashMap<String, Flow>();
    flowMap.put(flow.getId(), flow);
    project.setFlows(flowMap);
    ExecutableFlow execFlow = new ExecutableFlow(project, flow);

    return execFlow;
  }
}
