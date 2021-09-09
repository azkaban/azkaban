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
import static azkaban.common.ExecJettyServerModule.EXEC_CONTAINER_CONTEXT;
import static azkaban.common.ExecJettyServerModule.EXEC_JETTY_SERVER;
import static com.google.common.base.Preconditions.checkState;

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.Constants.PluginManager;
import azkaban.DispatchMethod;
import azkaban.cluster.ClusterModule;
import azkaban.cluster.ClusterRouter;
import azkaban.common.ExecJettyServerModule;
import azkaban.common.ServerUtils;
import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.AbstractFlowPreparer;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.ExecMetrics;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.TriggerManager;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.JobCallbackManager;
import azkaban.execapp.event.RemoteFlowWatcher;
import azkaban.execapp.jmx.JmxJobMBeanManager;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.imagemgmt.version.VersionSet;
import azkaban.jmx.JmxJettyServer;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.jobtype.HadoopProxy;
import azkaban.jobtype.JobTypeManager;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.project.ProjectLoader;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.server.AzkabanServer;
import azkaban.server.IMBeanRegistrable;
import azkaban.server.MBeanRegistrationManager;
import azkaban.sla.SlaOption;
import azkaban.spi.AzkabanEventReporter;
import azkaban.storage.ProjectStorageManager;
import azkaban.utils.DependencyTransferManager;
import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.Props;
import azkaban.utils.StdOutErrRedirect;
import azkaban.utils.Utils;
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
import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;

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
@Singleton
public class FlowContainer implements IMBeanRegistrable, EventListener<Event> {

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


  // Logging
  private static final Logger logger = Logger.getLogger(FlowContainer.class);
  private static final String logFileName = "logs/azkaban-execserver.log";
  private static final File logFile =
          new File(String.valueOf(ContainerizedFlowPreparer.getCurrentDir()), logFileName);

  private final ExecutorService executorService;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final TriggerManager triggerManager;
  private final JobTypeManager jobTypeManager;
  private final ClusterRouter clusterRouter;
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
  private final MBeanRegistrationManager mBeanRegistrationManager = new MBeanRegistrationManager();

  /**
   * Constructor of FlowContainer. It sets up all the DAO, all the loaders and Azkaban KeyStore.
   *
   * @param props Azkaban properties.
   * @param clusterRouter the router that decides which cluster a job should be submitted to
   * @throws IOException
   */
  @Inject
  public FlowContainer(final Props props,
      final ExecutorLoader executorLoader,
      final ProjectLoader projectLoader,
      final ClusterRouter clusterRouter,
      final TriggerManager triggerManager,
      @Nullable final AzkabanEventReporter eventReporter,
      @Named(EXEC_JETTY_SERVER) final Server jettyServer,
      @Named(EXEC_CONTAINER_CONTEXT) final Context context) throws ExecutorManagerException {

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
    this.clusterRouter = clusterRouter;
    this.triggerManager = triggerManager;
    this.triggerManager.setDispatchMethod(DispatchMethod.CONTAINERIZED);
    this.jobTypeManager =
        new JobTypeManager(
            this.azKabanProps.getString(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
                PluginManager.JOBTYPE_DEFAULTDIR),
            this.globalProps, getClass().getClassLoader(), clusterRouter,
            this.azKabanProps.getString(Constants.AZ_PLUGIN_LOAD_OVERRIDE_PROPS, null));

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
   * execution. It is assumed that AZ_HOME environment variable is set. If it is not set, then it
   * explicitly sets it to present working directory.
   *
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

    // Setup the callback mechanism and start the jetty server.
    flowContainer.start(azkabanProps);

    // Once submitFlow is called, the shutdown must happen for clean exit.
    try {
      // execute the flow, this is a blocking call until flow finishes
      flowContainer.submitFlow(execId);
    } catch (final ExecutorManagerException e) {
      // Log the cause
      logger.error("Flow execution failed due to ", e);
    } finally {
      // Shutdown the container
      flowContainer.shutdown();
    }
  }

  /**
   * Set Azkaban Props
   *
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
  static void setInjector(final Props azkabanProps) {
    // Inject AzkabanCommonModule
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(azkabanProps),
        new ClusterModule(),
        new ExecJettyServerModule()
    );
    SERVICE_PROVIDER.setInjector(injector);
  }

  /**
   * Submit flow Creates and submits the FlowRunner.
   *
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

    // Log the versionSet for this flow execution
    logVersionSet(flow);

    createFlowRunner(flow);
    submitFlowRunner();
  }

  /**
   * Create Flow Runner and setup the flow execution directory with project dependencies.
   *
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
    this.flowRunner = new FlowRunner(flow, this.executorLoader,
        this.projectLoader, this.jobTypeManager, this.azKabanProps, this.eventReporter,
        null, commonMetrics, execMetrics);
    this.flowRunner.setFlowWatcher(watcher)
        .setJobLogSettings(this.jobLogChunkSize, this.jobLogNumFiles)
        .setValidateProxyUser(this.validateProxyUser)
        .setNumJobThreads(this.numJobThreadPerFlow)
        .addListener(this);
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
   *
   * @throws ExecutorManagerException
   */
  private void setupKeyStore() throws ExecutorManagerException {
    // Fetch keyStore props and use it to get the KeyStore, put it in JobTypeManager
    final Props commonPluginLoadProps = this.jobTypeManager.getCommonPluginLoadProps();
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

  /**
   * Starts the Jetty Server and sets up callback mechanisms
   * @param azkabanProps Azkaban properties.
   */
  @VisibleForTesting
  void start(final Props azkabanProps) {
    this.containerContext.setAttribute(Constants.AZKABAN_CONTAINER_CONTEXT_KEY, this);
    JmxJobMBeanManager.getInstance().initialize(azkabanProps);

    ServerUtils.configureJobCallback(FlowContainer.logger, azkabanProps);
    configureMBeanServer();
    // Start the Jetty Server
    launchCtrlMsgListener(this);
  }

  public void cancelFlow(final int execId, final String user)
      throws ExecutorManagerException {

    logger.info("Cancel Flow called");
    if (this.flowRunner == null) {
      logger.warn(String.format("Attempt to cancel flow execId: %d before flow got a chance to start.",
          execId));
      throw new ExecutorManagerException("Flow has not launched yet.");
    }

    if (Status.isStatusFinished(this.flowRunner.getExecutableFlow().getStatus())) {
      logger.warn("Found a finished execution in the list of running flows: " + execId);
      throw new ExecutorManagerException("Execution is already finished.");
    }

    this.flowRunner.kill(user);
  }

  /**
   * Attempts to retry the failed jobs in a running execution.
   *
   * @param execId
   * @param user
   * @throws ExecutorManagerException
   */
  public void retryFailures(final int execId, final String user)
      throws ExecutorManagerException {

    if (this.flowRunner == null) {
      logger.warn(String.format("Attempt to retry failures for execId: %d before flow got a "
          + "chance to start.", execId));
      throw new ExecutorManagerException("Execution " + execId
          + " is not running.");
    }

    this.flowRunner.retryFailures(user);
  }

  /**
   * Return accumulated flow logs with the specified length from the flow container starting from
   * the given byte offset.
   *
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
      logger.warn(String.format("Attempt to read flow logs before flow execId: %d got a chance to start",
          execId));
      throw new ExecutorManagerException("The flow has not launched yet!");
    }

    final File dir = this.flowRunner.getExecutionDir();
    if (dir == null || !dir.exists()) {
      logger.warn(String.format("Error reading file. Execution directory does not exist for flow execId: %d",
          execId));
      throw new ExecutorManagerException("Error reading file. Execution directory does not exist");
    }

    try {
      final File logFile = FlowContainer.logFile;
      if (logFile.exists()) {
        return FileIOUtils.readUtf8File(logFile, startByte, length);
      } else {
        logger.warn(String.format("Flow log file does not exist for flow execId: %d", execId));
        throw new ExecutorManagerException("Flow log file does not exist.");
      }
    } catch (final IOException e) {
      logger.error(String.format("IOException while trying to read flow log file for flow execId: %d",
          execId));
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * Return accumulated job logs for a specific job starting with the provided byte offset.
   *
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
      logger.warn(String.format("Attempt to read job logs before flow got a chance to start. " +
          "Flow execId: %d, jobId: %s", execId, jobId));
      throw new ExecutorManagerException("The flow has not launched yet!");
    }

    final File dir = this.flowRunner.getExecutionDir();
    if (dir == null || !dir.exists()) {
      logger.warn(String.format("Error reading jobLogs. Execution dir does not exist. execId: %d, jobId: %s",
          execId, jobId));
      throw new ExecutorManagerException(
          "Error reading file. Execution directory does not exist.");
    }

    try {
      final File logFile = this.flowRunner.getJobLogFile(jobId, attempt);
      if (logFile != null && logFile.exists()) {
        return FileIOUtils.readUtf8File(logFile, startByte, length);
      } else {
        logger.warn(String.format("Job log file does not exist. Flow execId: %d, jobId: %s",
            execId, jobId));
        throw new ExecutorManagerException("Job log file does not exist.");
      }
    } catch (final IOException e) {
      logger.error(String.format("IOException while trying to read Job logs. execId: %d, jobId: %s",
          execId, jobId));
      throw new ExecutorManagerException(e);
    }
  }

  /**
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
      logger.warn(String.format("Metadata cannot be read as flow has not started. execId: %d, jobId: %s",
          execId, jobId));
      throw new ExecutorManagerException("The flow has not launched yet.");
    }

    final File dir = this.flowRunner.getExecutionDir();
    if (dir == null || !dir.exists()) {
      logger.warn(String.format("Execution directory does not exist. execId: %d, jobId: %s",
          execId, jobId));
      throw new ExecutorManagerException(
          "Error reading file. Execution directory does not exist.");
    }

    try {
      final File metaDataFile = this.flowRunner.getJobMetaDataFile(jobId, attempt);
      if (metaDataFile != null && metaDataFile.exists()) {
        return FileIOUtils.readUtf8MetaDataFile(metaDataFile, startByte, length);
      } else {
        logger.warn(String.format("Job metadata file does not exist. execId: %d, jobId: %s",
            execId, jobId));
        throw new ExecutorManagerException("Job metadata file does not exist.");
      }
    } catch (final IOException e) {
      logger.error(String.format("IOException while trying to read metadata file. execId: %d, jobId: %s",
          execId, jobId));
      throw new ExecutorManagerException(e);
    }
  }

  @VisibleForTesting
  static void launchCtrlMsgListener(final FlowContainer flowContainer) {
    try {
      flowContainer.jettyServer.start();
    } catch (final Exception e) {
      logger.error(e.getMessage());
    }

    final Connector[] connectors = flowContainer.jettyServer.getConnectors();
    checkState(connectors.length >= 1, "Server must have at least 1 connector");

    // The first connector is created upon initializing the server. That's the one that has the port.
    flowContainer.port = connectors[0].getLocalPort();
    logger.info(String.format("Listening on port %d for control messages.", flowContainer.port));
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
      logger.info(String.format("Execution ID : %d", execId));
    } catch (final NumberFormatException ne) {
      logger.error(String.format("Execution ID set in environment is invalid %s", execIdStr));
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
   * Log the versionSet for this flow execution
   * @param flow Executable flow.
   */
  private void logVersionSet(final ExecutableFlow flow) {
    final VersionSet versionSet = flow.getVersionSet();
    if (versionSet == null) {
      // Should not happen.
      logger.error("VersionSet is not set for the flow");
    } else {
      logger.info("VersionSet: " + ServerUtils.getVersionSetJsonString(versionSet));
    }
  }

  /**
   * This method configures the MBeanServer.
   */
  @Override
  public void configureMBeanServer() {
    logger.info("Registering MBeans...");

    this.mBeanRegistrationManager.registerMBean("executorJetty",
            new JmxJettyServer(this.jettyServer));
    this.mBeanRegistrationManager.registerMBean("jobJMXMBean", JmxJobMBeanManager.getInstance());

    if (JobCallbackManager.isInitialized()) {
      final JobCallbackManager jobCallbackMgr = JobCallbackManager.getInstance();
      this.mBeanRegistrationManager
              .registerMBean("jobCallbackJMXMBean", jobCallbackMgr.getJmxJobCallbackMBean());
    }
  }

  /**
   * Getter for MBeanRegistrationManager.
   * @return
   */
  @Override
  public MBeanRegistrationManager getMBeanRegistrationManager() {
    return this.mBeanRegistrationManager;
  }

  /**
   * Deletes all the symlinks and targeted files recursively.
   *
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
        logger.error(String.format("Error reading symlink %s", symlinkedFilePath), e);
        throw new ExecutorManagerException(e);
      }
      // Delete the symlink and then delete the symlinked file
      deleteSymlinkedFile(filePath);
    }
    // Delete the file, it could be a symlink
    try {
      Files.delete(symlinkedFilePath);
    } catch (final IOException e) {
      logger.error(String.format("Error deleting : %s", symlinkedFilePath), e);
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * Uploads the log file to the db for persistence.
   * @param execId execution id of the flow.
   */
  private void uploadLogFile(final int execId) {
    try {
      this.executorLoader.uploadLogFile(execId, "", 0, FlowContainer.logFile);
    } catch (final ExecutorManagerException e) {
      e.printStackTrace();
    }
  }

  /**
   * handleEvent : handles events related to flow state machine
   * @param : event emitted by FlowRunner
   */
  @Override
  public void handleEvent(final Event event) {
    if (event.getType().isFlowEventType()) {
      final FlowRunner flowRunner = (FlowRunner) event.getRunner();
      final ExecutableFlow flow = flowRunner.getExecutableFlow();
      // Set Flow level SLA options for containerized executions
      this.triggerManager
          .addTrigger(flow.getExecutionId(), SlaOption.getFlowLevelSLAOptions(flow
              .getExecutionOptions().getSlaOptions()));
    }
  }

  /**
   * Close the MBeans
   */
  @VisibleForTesting
  void closeMBeans() {
    this.mBeanRegistrationManager.closeMBeans();
  }

  /**
   * Shutdown the Container. This shuts down the ExecutorService which runs the flow execution as
   * well as JettyServer.
   */
  @VisibleForTesting
  void shutdown() {
    logger.info("Shutting down the container");
    if (this.flowRunner != null) {
      final int execId = this.flowRunner.getExecutionId();
      while (!this.flowFuture.isDone()) {
        // This should not happen immediately as submitFlowRunner is a blocking call.
        try {
          Thread.sleep(100);
        } catch (final InterruptedException e) {
          logger.error(String.format("The sleep while waiting for execution : %d to finish was interrupted", execId));
        }
      }
    } else {
      logger.warn("Flowrunner is null, the flow execution never started!");
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
    if (this.flowRunner != null) {
      // If the flowRunner is not created, the execId would be invalid.
      uploadLogFile(this.flowRunner.getExecutionId());
    }
    System.exit(0);
  }
}
