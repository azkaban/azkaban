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

import org.apache.log4j.Logger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import azkaban.utils.Props;

public class ProcessJobTest {
  private ProcessJob job = null;
  // private JobDescriptor descriptor = null;
  private Props props = null;
  private Logger log = Logger.getLogger(ProcessJob.class);

  @Before
  public void setUp() {

    /* initialize job */
    // props = EasyMock.createMock(Props.class);

    props = new Props();
    props.put(AbstractProcessJob.WORKING_DIR, ".");
    props.put("type", "command");
    props.put("fullPath", ".");

    // EasyMock.expect(props.getString("type")).andReturn("command").times(1);
    // EasyMock.expect(props.getProps()).andReturn(props).times(1);
    // EasyMock.expect(props.getString("fullPath")).andReturn(".").times(1);
    //
    // EasyMock.replay(props);

    job = new ProcessJob("TestProcess", props, props, log);

  }

  @Test
  public void testOneUnixCommand() throws Exception {
    /* initialize the Props */
    props.put(ProcessJob.COMMAND, "ls -al");
    props.put(ProcessJob.WORKING_DIR, ".");

    job.run();

  }

  @Test
  public void testFailedUnixCommand() throws Exception {
    /* initialize the Props */
    props.put(ProcessJob.COMMAND, "xls -al");
    props.put(ProcessJob.WORKING_DIR, ".");

    try {
      job.run();
    } catch (RuntimeException e) {
      Assert.assertTrue(true);
      e.printStackTrace();
    }
  }

  @Test
  public void testMultipleUnixCommands() throws Exception {
    /* initialize the Props */
    props.put(ProcessJob.WORKING_DIR, ".");
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
