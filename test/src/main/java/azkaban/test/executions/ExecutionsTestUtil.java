/*
 * Copyright 2015 Azkaban Authors
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

package azkaban.test.executions;

import java.io.File;

public class ExecutionsTestUtil {

  private static final String DATA_ROOT_PATH = "../test/execution-test-data/";

  public static String getDataRootDir() {
    // Assume that the working directory of a test is always the sub-module directory.
    // It is the case when running gradle tests from the project root directory.
    return DATA_ROOT_PATH;
  }


  public static File getFlowDir(final String flowName) {
    return new File(DATA_ROOT_PATH + File.separator + flowName);
  }

  public static File getFlowFile(final String flowName, final String fileName) {
    return new File(DATA_ROOT_PATH + File.separator + flowName + File.separator + fileName);
  }
}
