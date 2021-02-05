/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.container;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;
import static azkaban.common.ExecJettyServerModule.*;
import static com.google.common.base.Preconditions.*;

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.Constants.PluginManager;
import azkaban.common.ExecJettyServerModule;
import azkaban.execapp.AbstractFlowPreparer;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.ExecMetrics;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.RemoteFlowWatcher;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.imagemgmt.version.VersionSetLoader;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.jobtype.HadoopProxy;
import azkaban.jobtype.JobTypeManager;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.project.ProjectLoader;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.server.AzkabanServer;
import azkaban.spi.AzkabanEventReporter;
import azkaban.storage.ProjectStorageManager;
import azkaban.utils.*;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.FileIOUtils.JobMetaData;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This class is the entrypoint for launching a flow execution in a container.
 *  It sets up the Azkaban Properties, the DAO, injects all the required classes, sets up the
 *  execution directory along with the project files, creates the FlowRunner and submits it to
 *  the executor service for execution.
 *  It assumes that their is a certain directory structure consisting of all the dependencies.
 *  1.The dependencies such as Hadoop, Hive, Pig, and other libraries.
 *  2.The jobtype plugins are expected in "$AZ_HOME/plugins/jobtypes">
 *  3.The FlowContainer creates the project directory named "project" which contains all the
 *    project dependencies. It also serves as execution directory.
 *
 *  The flow execution's status is PREPARING when FlowContainer is called. It's status is set to
 *  RUNNING from FlowRunner. The rest of the state machine is handled by FlowRunner.
 */
public class FlowContainer {

  private static final String JOBTYPE_DIR = "plugins/jobtypes";
  private static final String CONF_ARG = "-conf";
  private static final String CONF_DIR = "conf";
  private static final String JOB_THREAD_COUNT = "flow.num.job.threads";
  private static final String DEFAULT_LOG_CHUNK_SIZE = "5MB";
  private static final int DEFAULT_LOG_NUM_FILES = 4;
  private static final int DEFAULT_JOB_TREAD_COUNT = 10;
  private static final boolean DEFAULT_USE_IN_MEMORY_KEYSTORE = false;
  // Should validate proxy user
  public static final boolean DEFAULT_VALIDATE_PROXY_USER = false;
  public static final String JOB_LOG_CHUNK_SIZE = "job.log.chunk.size";
  public static final String JOB_LOG_BACKUP_INDEX = "job.log.backup.index";
  public static final String PROXY_USER_LOCK_DOWN = "proxy.user.lock.down";
  private static final int SHUTDOWN_TIMEOUT_IN_SECONDS = 10;


  private static final Logger logger = LoggerFactory.getLogger(FlowContainer.class);

  private final ExecutorService executorService;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final JobTypeManager jobTypeManager;
  private final AbstractFlowPreparer flowPreparer;
  private final Server jettyServer;
  private final Context containerContext;
  private final AzkabanEventReporter eventReporter;
  private final Props azKabanProps;
  private Props globalProps;
  private final int numJobThreadPerFlow;
  private Path execDirPath;
  private int port; // Listener port for incoming control & log messages (ContainerServlet)
  private FlowRunner flowRunner;
  private Future<?> flowFuture;

  // Max chunk size is 20MB.
  private final String jobLogChunkSize;
  private final int jobLogNumFiles;
  // If true, jobs will validate proxy user against a list of valid proxy users.
  private final boolean validateProxyUser;

  /**
   * Constructor of FlowContainer.
   * It sets up all the DAO, all the loaders and Azkaban KeyStore.
   * @param props Azkaban properties.
   * @throws IOException
   */
  @Inject
  @Singleton
  public FlowContainer(final Props props,
      final ExecutorLoader executorLoader,
      final ProjectLoader projectLoader,
      @Nullable final AzkabanEventReporter eventReporter,
      @Named(EXEC_JETTY_SERVER) final Server jettyServer,
      @Named(EXEC_CONTAINER_CONTEXT) final Context context) throws ExecutorManagerException {

    // TODO : Figure out how to get VersionSetLoader and enable
    //logVersionSet(versionSetLoader);

    // Create Azkaban Props Map
    this.azKabanProps = props;
    // Setup global props if applicable
    final String globalPropsPath = this.azKabanProps.getString("executor.global.properties", null);
    if (globalPropsPath != null) {
      try {
        this.globalProps = new Props(null, globalPropsPath);
      } catch (final IOException e) {
        logger.error("Error creating global properties :" + globalPropsPath, e);
        throw new ExecutorManagerException(e);
      }
    }

    this.executorLoader = executorLoader;
    this.projectLoader = projectLoader;

    // setup executor service
    this.executorService = Executors.newSingleThreadExecutor();

    // jetty server
    this.jettyServer = jettyServer;
    this.containerContext = context;

    this.eventReporter = eventReporter;

    this.jobLogChunkSize = this.azKabanProps.getString(JOB_LOG_CHUNK_SIZE,
            DEFAULT_LOG_CHUNK_SIZE);
    this.jobLogNumFiles = this.azKabanProps.getInt(JOB_LOG_BACKUP_INDEX, DEFAULT_LOG_NUM_FILES);
    this.validateProxyUser = this.azKabanProps.getBoolean(PROXY_USER_LOCK_DOWN,
            DEFAULT_VALIDATE_PROXY_USER);
    this.jobTypeManager =
        new JobTypeManager(
            this.azKabanProps.getString(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
                PluginManager.JOBTYPE_DEFAULTDIR),
            this.globalProps, getClass().getClassLoader());

    this.numJobThreadPerFlow = props.getInt(JOB_THREAD_COUNT, DEFAULT_JOB_TREAD_COUNT);
    if (this.azKabanProps.getBoolean(Constants.USE_IN_MEMORY_KEYSTORE,
            DEFAULT_USE_IN_MEMORY_KEYSTORE)) {
      // Setting up the in-memory KeyStore for all the job executions in the flow.
      setupKeyStore();
    }
    // Create a flow preparer
    this.flowPreparer = new ContainerizedFlowPreparer(
            SERVICE_PROVIDER.getInstance(ProjectStorageManager.class),
            SERVICE_PROVIDER.getInstance(DependencyTransferManager.class));
  }

  /**
   * The entry point of FlowContainer. Validates the input arguments and submits the flow for
   * execution. It is assumed that AZ_HOME environment variable is set. If it is not set, then
   * it explicitly sets it to present working directory.
   * @param args Takes the execution id and Project zip file path as inputs.
   * @throws IOException
   * @throws ExecutorManagerException
   */
  public static void main(final String[] args) throws ExecutorManagerException {
    // Redirect all std out and err messages into slf4j
    StdOutErrRedirect.redirectOutAndErrToLog();
    // Get the execution ID from the environment
    final int execId = getExecutionId();
    final Path currentDir = ContainerizedFlowPreparer.getCurrentDir();

    // Set Azkaban props
    final Path jobtypePluginPath = Paths.get(currentDir.toString(), JOBTYPE_DIR);
    final Props azkabanProps = setAzkabanProps(jobtypePluginPath);

    // Setup Injector
    setInjector(azkabanProps);

    // Constructor
    final FlowContainer flowContainer = SERVICE_PROVIDER.getInstance(FlowContainer.class);
    flowContainer.start();
    launchCtrlMsgListener(flowContainer);

    // TODO : Revisit this logic with full implementation for JMXBEanManager and other callback mechanisms
    JmxJobMBeanManager.getInstance().initialize(azkabanProps);
    // execute the flow, this is a blocking call until flow finishes
    flowContainer.submitFlow(execId);
    // Shutdown the container
    flowContainer.shutdown();
  }

  /**
   * Set Azkaban Props
   * @param jobtypePluginPath Path where all the jobtype plugins are mounted.
   * @return Populated Azkaban properties.
   */
  private static Props setAzkabanProps(final Path jobtypePluginPath) {
    final Map<String, String> propsMap = new HashMap<>();
    propsMap.put(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
        jobtypePluginPath.toString());

    // Setup the azkaban.properties here.
    final String[] args = {CONF_ARG, CONF_DIR};
    final Props props = AzkabanServer.loadProps(args);

    return new Props(props, propsMap);
  }

  @VisibleForTesting
  static void setInjector(final Props azkabanProps){
    // Inject AzkabanCommonModule
    final Injector injector = Guice.createInjector(
            new AzkabanCommonModule(azkabanProps),
            new ExecJettyServerModule()
    );
    SERVICE_PROVIDER.setInjector(injector);
  }

  /**
   * Submit flow
   * Creates and submits the FlowRunner.
   * @param execId Execution Id of the flow.
   * @throws ExecutorManagerException
   */
  @VisibleForTesting
  void submitFlow(final int execId)
          throws ExecutorManagerException {
    final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(execId);
    if (flow == null) {
      logger.error("Error loading flow with execution Id " + execId);
      throw new ExecutorManagerException("Error loading flow for exec: " + execId +
              ". Terminating flow container launch");
    }

    createFlowRunner(flow);
    submitFlowRunner();
  }

  /**
   * Create Flow Runner and setup the flow execution directory with project dependencies.
   * @param flow Executable flow object.
   * @return FlowRunner object.
   * @throws ExecutorManagerException
   */
  private void createFlowRunner(final ExecutableFlow flow) throws ExecutorManagerException {
    // Prepare the flow with project dependencies.
    this.flowPreparer.setup(flow);

    // Setup flow watcher
    FlowWatcher watcher = null;
    final ExecutionOptions options = flow.getExecutionOptions();
    if (options.getPipelineExecutionId() != null) {
      final int pipelinedExecId = options.getPipelineExecutionId();
      watcher = new RemoteFlowWatcher(pipelinedExecId, this.executorLoader);
    }

    // TODO : figure out the metrics
    // Create the FlowRunner
    final MetricsManager metricsManager = new MetricsManager(new MetricRegistry());
    final CommonMetrics commonMetrics = new CommonMetrics(metricsManager);
    final ExecMetrics execMetrics = new ExecMetrics(metricsManager);
    final AzkabanEventReporter eventReporter =
            SERVICE_PROVIDER.getInstance(AzkabanEventReporter.class);
    this.flowRunner = new FlowRunner(flow, this.executorLoader,
        this.projectLoader, this.jobTypeManager, this.azKabanProps, eventReporter,
            null, commonMetrics, execMetrics);
    this.flowRunner.setFlowWatcher(watcher)
        .setJobLogSettings(this.jobLogChunkSize, this.jobLogNumFiles)
        .setValidateProxyUser(this.validateProxyUser)
        .setNumJobThreads(this.numJobThreadPerFlow);
  }

  /**
   * Submits the flow to executorService for execution.
   */
  private void submitFlowRunner() throws ExecutorManagerException {
    // set running flow, put it in DB
    logger.info("Submitting flow with execution Id " + this.flowRunner.getExecutionId());
    this.flowFuture = this.executorService.submit(this.flowRunner);
    try {
      // Blocking call
      this.flowFuture.get();
    } catch (final InterruptedException | ExecutionException e) {
      logger.error(ExceptionUtils.getStackTrace(e));
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * Setup in-memory keystore to be reused for all the job executions in the flow.
   * @throws ExecutorManagerException
   */
  private void setupKeyStore() throws ExecutorManagerException {
    // Fetch keyStore props and use it to get the KeyStore, put it in JobTypeManager
    Props commonPluginLoadProps = this.jobTypeManager.getCommonPluginLoadProps();
    if (commonPluginLoadProps != null) {
      // Load HadoopSecurityManager
      HadoopSecurityManager hadoopSecurityManager = null;
      try {
        final String hadoopSecurityClassName =
                commonPluginLoadProps.getString(HadoopJobUtils.HADOOP_SECURITY_MANAGER_CLASS_PARAM);
        final Class<?> hadoopSecurityManagerClass =
                HadoopProxy.class.getClassLoader().loadClass(hadoopSecurityClassName);

        logger.info("Loading hadoop security manager " + hadoopSecurityManagerClass.getName());
        hadoopSecurityManager = (HadoopSecurityManager)
                Utils.callConstructor(hadoopSecurityManagerClass, commonPluginLoadProps);
      } catch (final Exception e) {
        logger.error("Could not instantiate Hadoop Security Manager ", e);
        throw new RuntimeException("Failed to get hadoop security manager!"
                + e.getCause(), e);
      }

      final KeyStore keyStore = hadoopSecurityManager.getKeyStore(commonPluginLoadProps);
      if (keyStore == null) {
        logger.error("Failed to Prefetch KeyStore");
        throw new ExecutorManagerException("Failed to Prefetch KeyStore");
      }
      logger.info("In-memory Keystore is setup, delete the cert file");
      // Delete the cert file from disk as the KeyStore is already cached above.
      final Path certFilePath = Paths.get(this.azKabanProps.get(
              Constants.ConfigurationKeys.CSR_KEYSTORE_LOCATION));
      deleteSymlinkedFile(certFilePath);
    }
  }

  @VisibleForTesting
  void start() {
     this.containerContext.setAttribute(Constants.AZKABAN_CONTAINER_CONTEXT_KEY, this);
  }

  public void cancelFlow(final int execId, final String user)
    throws ExecutorManagerException {

    logger.info("Cancel Flow called");
    if (this.flowRunner == null) {
      logger.warn("Attempt to cancel flow execId: {} before flow got a chance to start.",
          execId);
      throw new ExecutorManagerException("Flow has not launched yet.");
    }

    if (Status.isStatusFinished(this.flowRunner.getExecutableFlow().getStatus())) {
      logger.warn("Found a finished execution in the list of running flows: " + execId);
      throw new ExecutorManagerException("Execution is already finished.");
    }

    this.flowRunner.kill(user);
  }

  /**
   * Return accumulated flow logs with the specified length from the flow container starting from the given byte offset.
   * @param execId
   * @param startByte
   * @param length
   * @return
   * @throws ExecutorManagerException
   */
  public LogData readFlowLogs(final int execId, final int startByte, final int length)
    throws ExecutorManagerException {
    logger.info("readFlowLogs called");
    if (this.flowRunner == null) {
      logger.warn("Attempt to read flow logs before flow execId: {} got a chance to start",
          execId);
      throw new ExecutorManagerException("The flow has not launched yet!");
    }

    final File dir = this.flowRunner.getExecutionDir();
    if (dir == null || !dir.exists()) {
      logger.warn("Error reading file. Execution directory does not exist for flow execId: {}", execId);
      throw new ExecutorManagerException("Error reading file. Execution directory does not exist");
    }

   try {
     final File logFile = this.flowRunner.getFlowLogFile();
     if (logFile != null && logFile.exists()) {
       return FileIOUtils.readUtf8File(logFile, startByte, length);
     } else {
       logger.warn("Flow log file does not exist for flow execId: {}", execId);
       throw new ExecutorManagerException("Flow log file does not exist.");
     }
   } catch (final IOException e) {
     logger.warn("IOException while trying to read flow log file for flow execId: {}",
         execId);
     throw new ExecutorManagerException(e);
   }
  }

  /**
   * Return accumulated job logs for a specific job starting with the provided byte offset.
   * @param execId
   * @param jobId
   * @param attempt
   * @param startByte
   * @param length
   * @return
   * @throws ExecutorManagerException
   */
  public LogData readJobLogs(final int execId, final String jobId, final int attempt,
      final int startByte, final int length) throws ExecutorManagerException {

    logger.info("readJobLogs called");
    if (this.flowRunner == null) {
      logger.warn("Attempt to read job logs before flow got a chance to start. " +
          "Flow execId: {}, jobId: {}", execId, jobId);
      throw new ExecutorManagerException("The flow has not launched yet!");
    }

    final File dir = this.flowRunner.getExecutionDir();
    if (dir == null || !dir.exists()) {
      logger.warn("Error reading jobLogs. Execution dir does not exist. execId: {}, jobId: {}",
          execId, jobId);
      throw new ExecutorManagerException(
          "Error reading file. Execution directory does not exist.");
    }

    try {
      final File logFile = this.flowRunner.getJobLogFile(jobId, attempt);
      if (logFile != null && logFile.exists()) {
        return FileIOUtils.readUtf8File(logFile, startByte, length);
      } else {
        logger.warn("Job log file does not exist. Flow execId: {}, jobId: {}",
            execId, jobId);
        throw new ExecutorManagerException("Job log file does not exist.");
      }
    } catch (final IOException e) {
      logger.warn("IOException while trying to read Job logs. execId: {}, jobId: {}",
          execId, jobId);
      throw new ExecutorManagerException(e);
    }
  }

  /**
   *
   * @param execId
   * @param jobId
   * @param attempt
   * @param startByte
   * @param length
   * @return
   * @throws ExecutorManagerException
   */
  public JobMetaData readJobMetaData(final int execId, final String jobId,
      final int attempt, final int startByte, final int length) throws ExecutorManagerException {

    logger.info("readJobMetaData called");
    if (this.flowRunner == null) {
      logger.warn("Metadata cannot be read as flow has not started. execId: {}, jobId: {}",
          execId, jobId);
      throw new ExecutorManagerException("The flow has not launched yet.");
    }

    final File dir = this.flowRunner.getExecutionDir();
    if (dir == null || !dir.exists()) {
      logger.warn("Execution directory does not exist. execId: {}, jobId: {}",
          execId, jobId);
      throw new ExecutorManagerException(
          "Error reading file. Execution directory does not exist.");
    }

    try {
      final File metaDataFile = this.flowRunner.getJobMetaDataFile(jobId, attempt);
      if (metaDataFile != null && metaDataFile.exists()) {
        return FileIOUtils.readUtf8MetaDataFile(metaDataFile, startByte, length);
      } else {
        logger.warn("Job metadata file does not exist. execId: {}, jobId: {}",
            execId, jobId);
        throw new ExecutorManagerException("Job metadata file does not exist.");
      }
    } catch (final IOException e) {
      logger.warn("IOException while trying to read metadata file. execId: {}, jobId: {}",
          execId, jobId);
      throw new ExecutorManagerException(e);
    }
  }

  @VisibleForTesting
  static void launchCtrlMsgListener(FlowContainer flowContainer) {
    try {
      flowContainer.jettyServer.start();
    } catch (final Exception e) {
      logger.error(e.getMessage());
    }
    // TODO Add hook for JobCallback

    final Connector[] connectors = flowContainer.jettyServer.getConnectors();
    checkState(connectors.length >= 1, "Server must have at least 1 connector");

    // The first connector is created upon initializing the server. That's the one that has the port.
    flowContainer.port = connectors[0].getLocalPort();
    logger.info("Listening on port {} for control messages.", flowContainer.port);
  }

  private static int getExecutionId() throws ExecutorManagerException {
    final String execIdStr = System.getenv(
            Constants.ContainerizedDispatchManagerProperties.ENV_FLOW_EXECUTION_ID);
    if (execIdStr == null) {
      final String msg = "Execution ID is not set!";
      logger.error(msg);
      throw new ExecutorManagerException(msg);
    }
    // Process Execution ID.
    int execId = -1;
    try {
      execId = Integer.parseInt(execIdStr);
      logger.info("Execution ID : {}", execId);
    } catch (final NumberFormatException ne) {
      logger.error("Execution ID set in environment is invalid {}", execIdStr);
      throw new ExecutorManagerException(ne);
    }
    if (execId < 1) {
      final String msg = "Invalid Execution ID : " + execId;
      logger.error(msg);
      throw new ExecutorManagerException(msg);
    }
    return execId;
  }


  /**
   * Log the VersionSet for the flow.
   * @param versionSetLoader
   */
  private void logVersionSet(final VersionSetLoader versionSetLoader) {
    // Get the version set id.
    final String versionSetIdStr = System.getenv(
            Constants.ContainerizedDispatchManagerProperties.ENV_VERSION_SET_ID);
    if (versionSetIdStr == null) {
      // Should not happen
      logger.warn("VersionSet ID is not set!");
      return;
    }
    int versionSetId = -1;
    try {
      versionSetId = Integer.parseInt(versionSetIdStr);
    } catch (final NumberFormatException ne) {
      logger.warn("VersionSet ID set in environment is invalid {}", versionSetIdStr);
      return;
    }
    VersionSet versionSet;
    try {
      versionSet = versionSetLoader.getVersionSetById(versionSetId).get();
    } catch (final IOException ioe) {
      logger.warn("Failed to fetch versionSet using versionSet ID : {}", versionSetId);
      return;
    }
    logger.info("VersionSet: {}", versionSet.getVersionSetJsonString());
  }

  /**
   * Deletes all the symlinks and targeted files recursively.
   * @param symlinkedFilePath Path to file, could be a symlink
   * @throws ExecutorManagerException
   */
  public static void deleteSymlinkedFile(final Path symlinkedFilePath)
          throws ExecutorManagerException {
    if (Files.isSymbolicLink(symlinkedFilePath)) {
      Path filePath = null;
      try {
        filePath = Files.readSymbolicLink(symlinkedFilePath);
      } catch (final IOException e) {
        logger.error("Error reading symlink {}", symlinkedFilePath, e);
        throw new ExecutorManagerException(e);
      }
      // Delete the symlink and then delete the symlinked file
      deleteSymlinkedFile(filePath);
    }
    // Delete the file, it could be a symlink
    try {
      Files.delete(symlinkedFilePath);
    } catch (final IOException e) {
      logger.error("Error deleting : {}", symlinkedFilePath, e);
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * Shutdown the Container. This shuts down the ExecutorService which runs the flow execution
   * as well as JettyServer.
   */
  private void shutdown() {
    logger.info("Shutting down the pod");
    while (!this.flowFuture.isDone()) {
      // This should not happen immediately as submitFlowRunner is a blocking call.
      try {
        Thread.sleep(100);
      } catch (final InterruptedException e) {
        logger.error("The sleep while waiting for execution : {} to finish was interrupted",
                this.flowRunner.getExecutionId());
      }
    }

    boolean result = false;
    try {
      this.executorService.shutdown();
      // Wait upto 10 seconds for clean shutdown, otherwise, System.exit() will wipe out
      // everything
      logger.info("Awaiting Shutdown of executing flow");
      result = this.executorService.awaitTermination(
              SHUTDOWN_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      logger.error(e.getMessage());
    }
    if (!result) {
      logger.warn("ExecutorService did not shut down cleanly yet. Ignoring it.");
    }

    try {
      this.jettyServer.stop();
      this.jettyServer.destroy();
    } catch (final Exception e) {
      // Eat up the exception
      logger.error("Error shutting down JettyServer while winding down the FlowContainer", e);
    }
    logger.info("Sayonara!");
    System.exit(0);
  }
}
