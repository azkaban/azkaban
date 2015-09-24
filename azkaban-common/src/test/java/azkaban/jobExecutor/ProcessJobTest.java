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

package azkaban.jobExecutor;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;

public class ProcessJobTest {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private ProcessJob job = null;
  private Props props = null;
  private Logger log = Logger.getLogger(ProcessJob.class);

  @Before
  public void setUp() throws IOException {
    File workingDir = temp.newFolder("TestProcess");

    props = new Props();
    props.put(AbstractProcessJob.WORKING_DIR, workingDir.getCanonicalPath());
    props.put("type", "command");
    props.put("fullPath", ".");
    
    props.put(CommonJobProperties.PROJECT_NAME, "test_project");
    props.put(CommonJobProperties.FLOW_ID, "test_flow");
    props.put(CommonJobProperties.JOB_ID, "test_job");
    props.put(CommonJobProperties.EXEC_ID, "123");
    props.put(CommonJobProperties.SUBMIT_USER, "test_user");

    job = new ProcessJob("TestProcess", props, props, log);
  }

  @After
  public void tearDown() {
    temp.delete();
  }

  @Test
  public void testOneUnixCommand() throws Exception {
    // Initialize the Props
    props.put(ProcessJob.COMMAND, "ls -al");
    job.run();

  }
  
  /**
   * this job should run fine if the props contain user.to.proxy
   * @throws Exception
   */
  @Test
  public void testOneUnixCommandWithProxyUserInsteadOfSubmitUser() throws Exception {
    
    // Initialize the Props
    props.removeLocal(CommonJobProperties.SUBMIT_USER);
    props.put("user.to.proxy", "test_user");
    props.put(ProcessJob.COMMAND, "ls -al");
    
    job.run();

  }
  
  /**
   * this job should fail because there is no user.to.proxy and no CommonJobProperties.SUBMIT_USER
   * @throws Exception
   */
  @Test (expected=RuntimeException.class)
  public void testOneUnixCommandWithNoUser() throws Exception {
    
    // Initialize the Props
    props.removeLocal(CommonJobProperties.SUBMIT_USER);    
    props.put(ProcessJob.COMMAND, "ls -al");
    
    job.run();

  }

  @Test
  public void testFailedUnixCommand() throws Exception {
    // Initialize the Props
    props.put(ProcessJob.COMMAND, "xls -al");

    try {
      job.run();
    } catch (RuntimeException e) {
      Assert.assertTrue(true);
      e.printStackTrace();
    }
  }

  @Test
  public void testMultipleUnixCommands() throws Exception {
    // Initialize the Props
    props.put(ProcessJob.COMMAND, "pwd");
    props.put("command.1", "date");
    props.put("command.2", "whoami");

    job.run();
  }

  @Test
  public void testPartitionCommand() throws Exception {
    String test1 = "a b c";

    Assert.assertArrayEquals(new String[] { "a", "b", "c" },
        ProcessJob.partitionCommandLine(test1));

    String test2 = "a 'b c'";
    Assert.assertArrayEquals(new String[] { "a", "b c" },
        ProcessJob.partitionCommandLine(test2));

    String test3 = "a e='b c'";
    Assert.assertArrayEquals(new String[] { "a", "e=b c" },
        ProcessJob.partitionCommandLine(test3));
  }
}
