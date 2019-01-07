/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.jobtype;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.ProcessJob;
import azkaban.utils.Props;

public class AllJobExecutorTests {

  public static Props setUpCommonProps() {

    final Props props = new Props();
    props.put("fullPath", ".");
    props.put(CommonJobProperties.PROJECT_NAME, "test_project");
    props.put(CommonJobProperties.FLOW_ID, "test_flow");
    props.put(CommonJobProperties.JOB_ID, "test_job");
    props.put(CommonJobProperties.EXEC_ID, "123");
    props.put(CommonJobProperties.SUBMIT_USER, "test_user");

    //The execute-as-user binary requires special permission. It's not convenient to 
    //set up in a unit test that is self contained. So EXECUTE_AS_USER is set to false 
    //so that we don't have to rely on the binary file to change user in the test case.
    props.put(ProcessJob.EXECUTE_AS_USER, "false");
    return props;
  }
}
