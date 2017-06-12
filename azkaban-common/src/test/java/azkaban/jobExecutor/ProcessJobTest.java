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

import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class ProcessJobTest {

  private final Logger log = Logger.getLogger(ProcessJob.class);
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private ProcessJob job = null;
  private Props props = null;

  @BeforeClass
  public static void classInit() throws Exception {
    azkaban.test.Utils.initServiceProvider();

  }

  @Before
  public void setUp() throws IOException {
    final File workingDir = this.temp.newFolder("TestProcess");

    // Initialize job
    this.props = AllJobExecutorTests.setUpCommonProps();
    this.props.put(AbstractProcessJob.WORKING_DIR, workingDir.getCanonicalPath());
    this.props.put("type", "command");

    this.job = new ProcessJob("TestProcess", this.props, this.props, this.log);
  }

  @After
  public void tearDown() {
    this.temp.delete();
  }

  @Test
  public void testOneUnixCommand() throws Exception {
    // Initialize the Props
    this.props.put(ProcessJob.COMMAND, "ls -al");
    this.job.run();

  }

  /**
   * this job should run fine if the props contain user.to.proxy
   */
  @Test
  public void testOneUnixCommandWithProxyUserInsteadOfSubmitUser() throws Exception {

    // Initialize the Props
    this.props.removeLocal(CommonJobProperties.SUBMIT_USER);
    this.props.put("user.to.proxy", "test_user");
    this.props.put(ProcessJob.COMMAND, "ls -al");

    this.job.run();

  }

  /**
   * this job should fail because there is no user.to.proxy and no CommonJobProperties.SUBMIT_USER
   */
  @Test(expected = RuntimeException.class)
  public void testOneUnixCommandWithNoUser() throws Exception {

    // Initialize the Props
    this.props.removeLocal(CommonJobProperties.SUBMIT_USER);
    this.props.put(ProcessJob.COMMAND, "ls -al");

    this.job.run();

  }

  @Test
  public void testFailedUnixCommand() throws Exception {
    // Initialize the Props
    this.props.put(ProcessJob.COMMAND, "xls -al");

    try {
      this.job.run();
    } catch (final RuntimeException e) {
      Assert.assertTrue(true);
      e.printStackTrace();
    }
  }

  @Test
  public void testMultipleUnixCommands() throws Exception {
    // Initialize the Props
    this.props.put(ProcessJob.COMMAND, "pwd");
    this.props.put("command.1", "date");
    this.props.put("command.2", "whoami");

    this.job.run();
  }

  @Test
  public void testPartitionCommand() throws Exception {
    final String test1 = "a b c";

    Assert.assertArrayEquals(new String[]{"a", "b", "c"},
        ProcessJob.partitionCommandLine(test1));

    final String test2 = "a 'b c'";
    Assert.assertArrayEquals(new String[]{"a", "b c"},
        ProcessJob.partitionCommandLine(test2));

    final String test3 = "a e='b c'";
    Assert.assertArrayEquals(new String[]{"a", "e=b c"},
        ProcessJob.partitionCommandLine(test3));
  }
}
