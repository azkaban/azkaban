/*
 * Copyright 2018 LinkedIn Corp.
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import azkaban.Constants.JobProperties;
import azkaban.flow.CommonJobProperties;
import azkaban.utils.Props;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    this.props.put(JobProperties.USER_TO_PROXY, "test_user");
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

  /**
   * this job should fail because it sets user.to.proxy = root which is black listed
   */
  @Test(expected = RuntimeException.class)
  public void testOneUnixCommandWithRootUser() throws Exception {

    // Initialize the Props
    this.props.removeLocal(CommonJobProperties.SUBMIT_USER);
    this.props.put(JobProperties.USER_TO_PROXY, "root");
    this.props.put("execute.as.user", "true");
    this.props.put(ProcessJob.COMMAND, "ls -al");

    this.job.run();

  }

  /**
   * this job should fail because it sets user.to.proxy = azkaban which is black listed
   */
  @Test(expected = RuntimeException.class)
  public void testOneUnixCommandWithAzkabanUser() throws Exception {

    // Initialize the Props
    this.props.removeLocal(CommonJobProperties.SUBMIT_USER);
    this.props.put(JobProperties.USER_TO_PROXY, "azkaban");
    this.props.put("execute.as.user", "true");
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

  /**
   * test cancellation of the job before associated process is constructed expect job will be
   * cancelled successfully
   */
  @Test
  public void testCancelDuringPreparation() throws InterruptedException, ExecutionException {
    final Props jobProps = new Props();
    jobProps.put("command", "echo hello");
    jobProps.put("working.dir", "/tmp");
    jobProps.put(JobProperties.USER_TO_PROXY, "test");
    jobProps.put("azkaban.flow.projectname", "test");
    jobProps.put("azkaban.flow.flowid", "test");
    jobProps.put("azkaban.job.id", "test");
    jobProps.put("azkaban.flow.execid", "1");

    final Props sysProps = new Props();
    sysProps.put("execute.as.user", "false");
    final SleepBeforeRunJob sleepBeforeRunJob = new SleepBeforeRunJob("test", sysProps, jobProps,
        this.log);

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final Future future = executorService.submit(sleepBeforeRunJob);
    sleepBeforeRunJob.cancel();
    future.get();
    assertThat(sleepBeforeRunJob.getProgress()).isEqualTo(0.0);
  }

  @Test
  public void testCancelAfterJobProcessCreation() throws InterruptedException, ExecutionException {
    this.props.put(ProcessJob.COMMAND, "sleep 5");

    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final Future future = executorService.submit(() -> {
      try {
        this.job.run();
      } catch (final Exception e) {
      }
    });

    // Wait for the AzkabanProcess object to be created before calling job.cancel so that the job
    // process is created.
    while (this.job.getProcess() == null) {
      Thread.sleep(1);
    }

    this.job.cancel();
    // Wait for the job to finish before testing its state.
    future.get();
    assertThat(this.job.isSuccess()).isFalse();
  }

  @Test
  public void testGeneratedProperties() throws Exception {

    String workingDir = this.props.get(AbstractProcessJob.WORKING_DIR);
    Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwx------");
    File script = new File(workingDir, "write_to_props.sh");
    Files.createFile(script.toPath(), PosixFilePermissions.asFileAttribute(perms));

    try (BufferedWriter w = Files.newBufferedWriter(script.toPath(), UTF_8)) {
      w.write("#!/bin/sh\n" +
        "echo '{\"props\":\"x,y,x\",\"str\":\"test\",\"arry\":[\"a\",\"b\"],\"obj\":{\"c\":[\"d\",\"e\"]}}' " +
        "> $JOB_OUTPUT_PROP_FILE\n");
    }

    this.props.put(ProcessJob.COMMAND, "./write_to_props.sh");

    this.job.run();

    Props generated = this.job.getJobGeneratedProperties();

    assertEquals("x,y,x", generated.get("props"));
    assertEquals("[\"a\",\"b\"]", generated.get("arry"));
    assertEquals("{\"c\":[\"d\",\"e\"]}", generated.get("obj"));
    assertEquals("test", generated.get("str"));

  }


  static class SleepBeforeRunJob extends ProcessJob implements Runnable {

    public SleepBeforeRunJob(final String jobId, final Props sysProps, final Props jobProps,
        final Logger log) {
      super(jobId, sysProps, jobProps, log);
    }

    @Override
    public void run() {
      try {
        info("sleep for some time before actually running the job");
        Thread.sleep(10);
        super.run();
      } catch (final Exception ex) {
        this.getLog().error(ex);
      }
    }
  }
}
