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
package azkaban.jobExecutor;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_GROUP_NAME;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER;
import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.Constants;
import azkaban.Constants.JobProperties;
import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.utils.process.AzkabanProcess;
import azkaban.jobExecutor.utils.process.AzkabanProcessBuilder;
import azkaban.metrics.CommonMetrics;
import azkaban.utils.ExecuteAsUser;
import azkaban.utils.Pair;
import azkaban.utils.Props;
import azkaban.utils.SystemMemoryInfo;
import azkaban.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;


/**
 * A job that runs a simple unix command
 */
public class ProcessJob extends AbstractProcessJob {

  public static final String COMMAND = "command";
  public static final String AZKABAN_MEMORY_CHECK = "azkaban.memory.check";
  // Use azkaban.Constants.ConfigurationKeys.AZKABAN_SERVER_NATIVE_LIB_FOLDER instead
  @Deprecated
  public static final String NATIVE_LIB_FOLDER = "azkaban.native.lib";
  public static final String EXECUTE_AS_USER = "execute.as.user";
  public static final String KRB5CCNAME = "KRB5CCNAME";
  private static final Duration KILL_TIME = Duration.ofSeconds(30);
  private static final String MEMCHECK_ENABLED = "memCheck.enabled";
  private static final String CHOWN = "/bin/chown";
  private static final String CREATE_FILE = "touch";
  private static final int SUCCESSFUL_EXECUTION = 0;
  private static final String TEMP_FILE_NAME = "user_can_write";

  private final CommonMetrics commonMetrics;
  private volatile AzkabanProcess process;
  private volatile boolean killed = false;
  // For testing only. True if the job process exits successfully.
  private volatile boolean success;

  public ProcessJob(final String jobId, final Props sysProps,
      final Props jobProps, final Logger log) {
    super(jobId, sysProps, jobProps, log);
    // TODO: reallocf fully guicify CommonMetrics through ProcessJob dependents
    this.commonMetrics = SERVICE_PROVIDER.getInstance(CommonMetrics.class);
  }

  public ProcessJob(final String jobId, final Props sysProps,
      final Props jobProps, final Props privateProps, final Logger log) {
    super(jobId, sysProps, jobProps, privateProps, log);
    // TODO: reallocf fully guicify CommonMetrics through ProcessJob dependents
    this.commonMetrics = SERVICE_PROVIDER.getInstance(CommonMetrics.class);
  }

  /**
   * Splits the command into a unix like command line structure. Quotes and single quotes are
   * treated as nested strings.
   */
  public static String[] partitionCommandLine(final String command) {
    final ArrayList<String> commands = new ArrayList<>();

    int index = 0;

    StringBuffer buffer = new StringBuffer(command.length());

    boolean isApos = false;
    boolean isQuote = false;
    while (index < command.length()) {
      final char c = command.charAt(index);

      switch (c) {
        case ' ':
          if (!isQuote && !isApos) {
            final String arg = buffer.toString();
            buffer = new StringBuffer(command.length() - index);
            if (arg.length() > 0) {
              commands.add(arg);
            }
          } else {
            buffer.append(c);
          }
          break;
        case '\'':
          if (!isQuote) {
            isApos = !isApos;
          } else {
            buffer.append(c);
          }
          break;
        case '"':
          if (!isApos) {
            isQuote = !isQuote;
          } else {
            buffer.append(c);
          }
          break;
        default:
          buffer.append(c);
      }

      index++;
    }

    if (buffer.length() > 0) {
      final String arg = buffer.toString();
      commands.add(arg);
    }

    return commands.toArray(new String[commands.size()]);
  }

  @Override
  public void run() throws Exception {
    try {
      resolveProps();
    } catch (final Exception e) {
      handleError("Bad property definition! " + e.getMessage(), e);
    }

    if (this.getSysProps().getBoolean(MEMCHECK_ENABLED, true)
        && this.getJobProps().getBoolean(AZKABAN_MEMORY_CHECK, true)) {
      final Pair<Long, Long> memPair = getProcMemoryRequirement();
      final long xms = memPair.getFirst();
      final long xmx = memPair.getSecond();
      // retry backoff in ms
      final String oomMsg = String
          .format("Cannot request memory (Xms %d kb, Xmx %d kb) from system for job %s",
              xms, xmx, getId());
      int attempt;
      boolean isMemGranted = true;

      //todo HappyRay: move to proper Guice after this class is refactored.
      final SystemMemoryInfo memInfo = SERVICE_PROVIDER.getInstance(SystemMemoryInfo.class);
      for (attempt = 1; attempt <= Constants.MEMORY_CHECK_RETRY_LIMIT; attempt++) {
        isMemGranted = memInfo.canSystemGrantMemory(xmx);
        if (isMemGranted) {
          info(String.format("Memory granted for job %s", getId()));
          if (attempt > 1) {
            this.commonMetrics.decrementOOMJobWaitCount();
          }
          break;
        }
        if (attempt < Constants.MEMORY_CHECK_RETRY_LIMIT) {
          info(String.format(oomMsg + ", sleep for %s secs and retry, attempt %s of %s",
              TimeUnit.MILLISECONDS.toSeconds(
                  Constants.MEMORY_CHECK_INTERVAL_MS), attempt,
              Constants.MEMORY_CHECK_RETRY_LIMIT));
          if (attempt == 1) {
            this.commonMetrics.incrementOOMJobWaitCount();
          }
          synchronized (this) {
            try {
              this.wait(Constants.MEMORY_CHECK_INTERVAL_MS);
            } catch (final InterruptedException e) {
              info(String
                  .format("Job %s interrupted while waiting for memory check retry", getId()));
            }
          }
          if (this.killed) {
            this.commonMetrics.decrementOOMJobWaitCount();
            info(String.format("Job %s was killed while waiting for memory check retry", getId()));
            return;
          }
        }
      }

      if (!isMemGranted) {
        this.commonMetrics.decrementOOMJobWaitCount();
        handleError(oomMsg, null);
      }
    }

    List<String> commands = null;
    try {
      commands = getCommandList();
    } catch (final Exception e) {
      handleError("Job set up failed: " + e.getMessage(), e);
    }

    final long startMs = System.currentTimeMillis();

    if (commands == null) {
      handleError("There are no commands to execute", null);
    }

    info(commands.size() + " commands to execute.");
    final File[] propFiles = initPropsFiles();

    // change krb5ccname env var so that each job execution gets its own cache
    final Map<String, String> envVars = getEnvironmentVariables();
    envVars.put(KRB5CCNAME, getKrb5ccname(this.getJobProps()));

    // determine whether to run as Azkaban or run as effectiveUser,
    // by default, run as effectiveUser
    String executeAsUserBinaryPath = null;
    String effectiveUser = null;
    final boolean isExecuteAsUser = this.getSysProps().getBoolean(EXECUTE_AS_USER, true);

    //Get list of users we never execute flows as. (ie: root, azkaban)
    final Set<String> blackListedUsers = new HashSet<>(
        Arrays.asList(
            this.getSysProps()
                .getString(Constants.ConfigurationKeys.BLACK_LISTED_USERS, "root,azkaban")
                .split(",")
        )
    );

    // nativeLibFolder specifies the path for execute-as-user file,
    // which will change user from Azkaban to effectiveUser
    if (isExecuteAsUser) {
      final String nativeLibFolder = this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER);
      executeAsUserBinaryPath = String.format("%s/%s", nativeLibFolder, "execute-as-user");
      effectiveUser = getEffectiveUser(this.getJobProps());
      // Throw exception if Azkaban tries to run flow as a prohibited user
      if (blackListedUsers.contains(effectiveUser)) {
        throw new RuntimeException(
            String.format("Not permitted to proxy as '%s' through Azkaban", effectiveUser)
        );
      }

      // Set parent directory permissions to <uid>:azkaban so user can write in their execution directory
      // if the directory does not have correct permission already (should happen once per
      // execution)
      if (!canWriteInCurrentWorkingDirectory(effectiveUser)) {
        info("Changing current working directory ownership");
        assignUserFileOwnership(effectiveUser, getWorkingDirectory());
      }
      // Set property file permissions to <uid>:azkaban so user can write to their prop files
      // in order to pass properties from one job to another, except the last one
      for (int i = 0; i < 2; i++) {
        info("Changing properties files ownership");
        assignUserFileOwnership(effectiveUser, propFiles[i].getAbsolutePath());
      }
    }

    for (String command : commands) {
      AzkabanProcessBuilder builder = null;
      if (isExecuteAsUser) {
        command =
            String.format("%s %s %s", executeAsUserBinaryPath, effectiveUser,
                command);
        info("Command: " + command);
        builder =
            new AzkabanProcessBuilder(partitionCommandLine(command))
                .setEnv(envVars).setWorkingDir(getCwd()).setLogger(getLog())
                .enableExecuteAsUser().setExecuteAsUserBinaryPath(executeAsUserBinaryPath)
                .setEffectiveUser(effectiveUser);
      } else {
        info("Command: " + command);
        builder =
            new AzkabanProcessBuilder(partitionCommandLine(command))
                .setEnv(envVars).setWorkingDir(getCwd()).setLogger(getLog());
      }

      if (builder.getEnv().size() > 0) {
        info("Environment variables: " + builder.getEnv());
      }
      info("Working directory: " + builder.getWorkingDir());

      // print out the Job properties to the job log.
      this.logJobProperties();

      synchronized (this) {
        // Make sure that checking if the process job is killed and creating an AzkabanProcess
        // object are atomic. The cancel method relies on this to make sure that if this.process is
        // not null, this block of code which includes checking if the job is killed has not been
        // executed yet.
        if (this.killed) {
          info("The job is killed. Abort. No job process created.");
          return;
        }
        this.process = builder.build();
      }
      try {
        this.process.run();
        this.success = true;
      } catch (final Throwable e) {
        for (final File file : propFiles) {
          if (file != null && file.exists()) {
            file.delete();
          }
        }
        throw new RuntimeException(e);
      } finally {
        info("Process with id " + this.process.getProcessId() + " completed "
            + (this.success ? "successfully" : "unsuccessfully") + " in "
            + ((System.currentTimeMillis() - startMs) / 1000) + " seconds.");
      }
    }

    // Get the output properties from this job.
    generateProperties(propFiles[1]);
  }

  /**
   * <pre>
   * This method extracts the kerberos ticket cache file name from the jobprops.
   * This method will ensure that each job execution will have its own kerberos ticket cache file
   * Given that the code only sets an environmental variable, the number of files created
   * corresponds
   * to the number of processes that are doing kinit in their flow, which should not be an
   * inordinately
   * high number.
   * </pre>
   *
   * @return file name: the kerberos ticket cache file to use
   */
  private String getKrb5ccname(final Props jobProps) {
    final String effectiveUser = getEffectiveUser(jobProps);
    final String projectName =
        jobProps.getString(CommonJobProperties.PROJECT_NAME).replace(" ", "_");
    final String flowId =
        jobProps.getString(CommonJobProperties.FLOW_ID).replace(" ", "_");
    final String jobId =
        jobProps.getString(CommonJobProperties.JOB_ID).replace(" ", "_");
    // execId should be an int and should not have space in it, ever
    final String execId = jobProps.getString(CommonJobProperties.EXEC_ID);
    final String krb5ccname =
        String.format("/tmp/krb5cc__%s__%s__%s__%s__%s", projectName, flowId,
            jobId, execId, effectiveUser);

    return krb5ccname;
  }

  /**
   * <pre>
   * Determines what user id should the process job run as, in the following order of precedence:
   * 1. USER_TO_PROXY
   * 2. SUBMIT_USER
   * </pre>
   *
   * @return the user that Azkaban is going to execute as
   */
  private String getEffectiveUser(final Props jobProps) {
    String effectiveUser = null;
    if (jobProps.containsKey(JobProperties.USER_TO_PROXY)) {
      effectiveUser = jobProps.getString(JobProperties.USER_TO_PROXY);
    } else if (jobProps.containsKey(CommonJobProperties.SUBMIT_USER)) {
      effectiveUser = jobProps.getString(CommonJobProperties.SUBMIT_USER);
    } else {
      throw new RuntimeException(
          "Internal Error: No user.to.proxy or submit.user in the jobProps");
    }
    info("effective user is: " + effectiveUser);
    return effectiveUser;
  }

  /**
   * Checks to see if user has write access to current working directory which many users need for
   * their jobs to store temporary data/jars on the executor.
   *
   * Accomplishes this by using execute-as-user to try to create an empty file in the cwd.
   *
   * @param effectiveUser user/proxy user running the job
   * @return true if user has write permissions in current working directory otherwise false
   */
  private boolean canWriteInCurrentWorkingDirectory(final String effectiveUser)
      throws IOException {
    final ExecuteAsUser executeAsUser = new ExecuteAsUser(
        this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));
    final List<String> checkIfUserCanWriteCommand = Arrays
        .asList(CREATE_FILE, getWorkingDirectory() + "/" + TEMP_FILE_NAME);
    final int result = executeAsUser.execute(effectiveUser, checkIfUserCanWriteCommand);
    // There's bug when traversing a special DAG like A->A->B->A, where A and B are different
    // effective users. The CWD ownership would be A->A->B->B instead of A->A->B->A.
    // This is Because the TEMP_FILE created in the second job would persist (which A always has
    // write permission of this temp file) and violates the logic to check whether current effective
    // user has write permission in CWD.
    // Therefore, if TEMP_FILE is created, it should be destroyed at the end of the function
    try {
      Files.deleteIfExists(Paths.get(getWorkingDirectory() + "/" + TEMP_FILE_NAME));
    } catch (Exception e) {
      info(String.format("Failed to delete %s in current working directory", TEMP_FILE_NAME));
    }
    return result == SUCCESSFUL_EXECUTION;
  }

  /**
   * Changes permissions on file/directory so that the file/directory is owned by the user and the
   * group remains the azkaban service account name.
   *
   * Leverages execute-as-user with "root" as the user to run the command.
   *
   * @param effectiveUser user/proxy user running the job
   * @param fileName the name of the file whose permissions will be changed
   */
  private void assignUserFileOwnership(final String effectiveUser, final String fileName) throws
      Exception {
    final ExecuteAsUser executeAsUser = new ExecuteAsUser(
        this.getSysProps().getString(AZKABAN_SERVER_NATIVE_LIB_FOLDER));
    final String groupName = this.getSysProps().getString(AZKABAN_SERVER_GROUP_NAME, "azkaban");
    final List<String> changeOwnershipCommand = Arrays
        .asList(CHOWN, effectiveUser + ":" + groupName, fileName);
    info("Change ownership of " + fileName + " to " + effectiveUser + ":" + groupName + ".");
    final int result = executeAsUser.execute("root", changeOwnershipCommand);
    if (result != 0) {
      handleError("Failed to change current working directory ownership. Error code: " + Integer
          .toString(result), null);
    }
  }

  /**
   * This is used to get the min/max memory size requirement by processes. SystemMemoryInfo can use
   * the info to determine if the memory request can be fulfilled. For Java process, this should be
   * Xms/Xmx setting.
   *
   * @return pair of min/max memory size
   */
  protected Pair<Long, Long> getProcMemoryRequirement() throws Exception {
    return new Pair<>(0L, 0L);
  }

  protected void handleError(final String errorMsg, final Exception e) throws Exception {
    error(errorMsg);
    if (e != null) {
      throw new Exception(errorMsg, e);
    } else {
      throw new Exception(errorMsg);
    }
  }

  protected List<String> getCommandList() {
    final List<String> commands = new ArrayList<>();
    commands.add(this.getJobProps().getString(COMMAND));
    for (int i = 1; this.getJobProps().containsKey(COMMAND + "." + i); i++) {
      commands.add(this.getJobProps().getString(COMMAND + "." + i));
    }

    return commands;
  }

  @Override
  public void cancel() throws InterruptedException {
    // in case the job is waiting
    synchronized (this) {
      this.killed = true;
      this.notify();
      if (this.process == null) {
        // The job thread has not checked if the job is killed yet.
        // setting the killed flag should be enough to abort the job.
        // There is no job process to kill.
        return;
      }
    }
    this.process.awaitStartup();
    final boolean processkilled = this.process
        .softKill(KILL_TIME.toMillis(), TimeUnit.MILLISECONDS);
    if (!processkilled) {
      warn("Kill with signal TERM failed. Killing with KILL signal.");
      this.process.hardKill();
    }
  }

  @Override
  public double getProgress() {
    return this.process != null && this.process.isComplete() ? 1.0 : 0.0;
  }

  public int getProcessId() {
    return this.process.getProcessId();
  }

  @VisibleForTesting
  boolean isSuccess() {
    return this.success;
  }

  @VisibleForTesting
  AzkabanProcess getProcess() {
    return this.process;
  }

  public String getPath() {
    return Utils.ifNull(this.getJobPath(), "");
  }
}
