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
