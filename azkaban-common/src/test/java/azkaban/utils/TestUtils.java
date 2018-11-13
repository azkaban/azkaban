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
 */

package azkaban.utils;

import azkaban.executor.ExecutableFlow;
import azkaban.flow.Flow;
import azkaban.project.DirectoryYamlFlowLoader;
import azkaban.project.Project;
import azkaban.test.executions.ExecutionsTestUtil;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.XmlUserManager;
import com.google.common.base.Charsets;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

/**
 * Commonly used utils method for unit/integration tests
 */
public class TestUtils {

  public static User getTestUser() {
    return new User("testUser");
  }

  /* Helper method to create an ExecutableFlow from serialized description */
  public static ExecutableFlow createTestExecutableFlow(final String projectName,
      final String flowName) throws IOException {
    final File jsonFlowFile = ExecutionsTestUtil.getFlowFile(projectName, flowName + ".flow");
    final HashMap<String, Object> flowObj =
        (HashMap<String, Object>) JSONUtils.parseJSONFromFile(jsonFlowFile);

    final Flow flow = Flow.flowFromObject(flowObj);
    final Project project = new Project(1, "flow");
    final HashMap<String, Flow> flowMap = new HashMap<>();
    flowMap.put(flow.getId(), flow);
    project.setFlows(flowMap);
    final ExecutableFlow execFlow = new ExecutableFlow(project, flow);

    return execFlow;
  }

  /* Helper method to create an ExecutableFlow from Yaml */
  public static ExecutableFlow createTestExecutableFlowFromYaml(final String projectName,
      final String flowName) throws IOException {

    final Project project = new Project(11, projectName);
    final DirectoryYamlFlowLoader loader = new DirectoryYamlFlowLoader(new Props());
    loader.loadProjectFlow(project, ExecutionsTestUtil.getFlowDir(projectName));
    project.setFlows(loader.getFlowMap());
    project.setVersion(123);

    final Flow flow = project.getFlow(flowName);
    return new ExecutableFlow(project, flow);
  }

  /* Helper method to create an XmlUserManager from XML_FILE_PARAM file */
  public static UserManager createTestXmlUserManager() {
    final Props props = new Props();
    props.put(XmlUserManager.XML_FILE_PARAM, ExecutionsTestUtil.getDataRootDir()
        + "azkaban-users.xml");
    final UserManager manager = new XmlUserManager(props);
    return manager;
  }

  /**
   * Reads a resource into a String
   *
   * @param name Relative path to the resource (relative to the parent object's package)
   * @param parent Instance of the class to use in finding the resource. The resource is looked up in the same package
   * where the class of the parent object is in.
   * @return Resource content as a String
   * @throws IOException if an I/O error occurs
   */
  public static String readResource(final String name, final Object parent) throws IOException {
    try (final InputStream is = parent.getClass().getResourceAsStream(name)) {
      return IOUtils.toString(is, Charsets.UTF_8).trim();
    }
  }

}
