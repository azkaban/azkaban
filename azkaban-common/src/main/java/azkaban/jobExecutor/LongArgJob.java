/*
 * Copyright 2012 LinkedIn Corp.
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

import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.jobExecutor.utils.process.AzkabanProcessBuilder;
import azkaban.utils.Props;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

/**
 * A job that passes all the job properties as command line arguments in "long" format, e.g. --key1
 * value1 --key2 value2 ...
 */
public abstract class LongArgJob extends AbstractProcessJob {

  private static final long KILL_TIME_MS = 5000;
  private final AzkabanProcessBuilder builder;
  private volatile AzkabanProcess process;

  public LongArgJob(final String jobid, final String[] command, final Props sysProps,
      final Props jobProps, final Logger log) {
    this(jobid, command, sysProps, jobProps, log, new HashSet<>(0));
  }

  public LongArgJob(final String jobid, final String[] command, final Props sysProps,
      final Props jobProp, final Logger log, final Set<String> suppressedKeys) {
    super(jobid, sysProps, jobProp, log);

    this.builder =
        new AzkabanProcessBuilder(command)
            .setEnv(getJobProps().getMapByPrefix(ENV_PREFIX))
            .setWorkingDir(getCwd()).setLogger(getLog());
    appendProps(suppressedKeys);
  }

  @Override
  public void run() throws Exception {
    try {
      resolveProps();
    } catch (final Exception e) {
      error("Bad property definition! " + e.getMessage());
    }

    final long startMs = System.currentTimeMillis();
    info("Command: " + this.builder.getCommandString());
    if (this.builder.getEnv().size() > 0) {
      info("Environment variables: " + this.builder.getEnv());
    }
    info("Working directory: " + this.builder.getWorkingDir());

    final File[] propFiles = initPropsFiles();

    // print out the Job properties to the job log.
    this.logJobProperties();

    boolean success = false;
    this.process = this.builder.build();
    try {
      this.process.run();
      success = true;
    } catch (final Exception e) {
      for (final File file : propFiles) {
        if (file != null && file.exists()) {
          file.delete();
        }
      }
      throw new RuntimeException(e);
    } finally {
      this.process = null;
      info("Process completed " + (success ? "successfully" : "unsuccessfully")
          + " in " + ((System.currentTimeMillis() - startMs) / 1000)
          + " seconds.");
    }

    // Get the output properties from this job.
    generateProperties(propFiles[1]);

    for (final File file : propFiles) {
      if (file != null && file.exists()) {
        file.delete();
      }
    }
  }

  /**
   * This gives access to the process builder used to construct the process. An overriding class can
   * use this to add to the command being executed.
   */
  protected AzkabanProcessBuilder getBuilder() {
    return this.builder;
  }

  @Override
  public void cancel() throws InterruptedException {
    if (this.process == null) {
      throw new IllegalStateException("Not started.");
    }

    final boolean killed = this.process.softKill(KILL_TIME_MS, TimeUnit.MILLISECONDS);
    if (!killed) {
      warn("Kill with signal TERM failed. Killing with KILL signal.");
      this.process.hardKill();
    }
  }

  @Override
  public double getProgress() {
    return this.process != null && this.process.isComplete() ? 1.0 : 0.0;
  }

  private void appendProps(final Set<String> suppressed) {
    final AzkabanProcessBuilder builder = this.getBuilder();
    final Props props = getJobProps();
    for (final String key : props.getKeySet()) {
      if (!suppressed.contains(key)) {
        builder.addArg("--" + key, props.get(key));
      }
    }
  }
}
