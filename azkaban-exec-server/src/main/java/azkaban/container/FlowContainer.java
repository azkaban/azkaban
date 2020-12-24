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
import azkaban.executor.JdbcExecutorLoader;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.jobtype.HadoopProxy;
import azkaban.jobtype.JobTypeManager;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.server.AzkabanServer;
import azkaban.spi.AzkabanEventReporter;
import azkaban.user.User;
import azkaban.utils.FileIOUtils;
import azkaban.utils.FileIOUtils.LogData;
import azkaban.utils.FileIOUtils.JobMetaData;
import azkaban.utils.Props;
import azkaban.utils.StdOutErrRedirect;
import azkaban.utils.Utils;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.kubernetes.client.Exec;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipFile;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

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
 *  The Flow's status is DISPATCHING when FlowContainer is called. It's status is set to
 *  PREPARING before FlowRunner is created. The rest of the state machine is handled by FlowRunner.
 */
public class FlowContainer {

  private static final String PROJECT_DIR = "project";
  private static final String JOBTYPE_DIR = "plugins/jobtypes";
  private static final String CONF_ARG = "-conf";
  private static final String CONF_DIR = "conf";
  private static final String JOB_THREAD_COUNT = "flow.num.job.threads";
  private static final String DEFAULT_LOG_CHUNK_SIZE = "5MB";
  private static final int DEFAULT_LOG_NUM_FILES = 4;
  private static final int EXEC_ID_INDEX = 0;
  private static final int PROJECT_LOCATION_INDEX = 1;
  private static final int DEFAULT_JOB_TREAD_COUNT = 10;
  private static final String AZ_HOME = "AZ_HOME";
  private static final boolean DEFAULT_USE_IN_MEMORY_KEYSTORE = false;
  // Should validate proxy user
  public static final boolean DEFAULT_VALIDATE_PROXY_USER = false;

  private static final Logger logger = LoggerFactory.getLogger(FlowContainer.class);

  private final ExecutorService executorService;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final JobTypeManager jobTypeManager;
  private final Server jettyServer;
  private final Context containerContext;
  private final AzkabanEventReporter eventReporter;
  private final Props azKabanProps;
  private Props globalProps;
  private final int numJobThreadPerFlow;
  private String execDirPath;
  private int port; // Listener port for incoming control & log messages (ContainerServlet)
  private FlowRunner flowRunner;
  private ExecutableFlow flow; // A flow container is tasked to only run a single flow

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
      final AzkabanEventReporter eventReporter,
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
    logger.info("executorLoader from guice :" + this.executorLoader);

    // project Loader
    this.projectLoader = projectLoader;
    logger.info("projectLoader from guice : " + this.projectLoader);

    // setup executor service
    this.executorService = Executors.newSingleThreadExecutor();

    // jetty server
    this.jettyServer = jettyServer;
    this.containerContext = context;

    this.eventReporter = eventReporter;

    this.jobLogChunkSize = this.azKabanProps.getString("job.log.chunk.size",
            DEFAULT_LOG_CHUNK_SIZE);
    this.jobLogNumFiles = this.azKabanProps.getInt("job.log.backup.index", DEFAULT_LOG_NUM_FILES);
    this.validateProxyUser = this.azKabanProps.getBoolean("proxy.user.lock.down",
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
  }

  /**
   * The entry point of FlowContainer. Validates the input arguments and submits the flow for
   * execution. It is assumed that AZ_HOME environment variable is set. If it is not set, then
   * it explicitely sets it to present working directory.
   * @param args Takes the execution id and Project zip file path as inputs.
   * @throws IOException
   * @throws ExecutorManagerException
   */
  public static void main(final String[] args) throws ExecutorManagerException {
    // Redirect all std out and err messages into slf4j
    StdOutErrRedirect.redirectOutAndErrToLog();
    // Get all the arguments
    final String execIdStr = args[EXEC_ID_INDEX];
    final String projectZipName = args[PROJECT_LOCATION_INDEX];
    // Process Execution ID.
    int execId = -1;
    try {
      execId = Integer.parseInt(execIdStr);
    } catch (NumberFormatException ne) {
      logger.error("Execution ID passed in argument is invalid {}", execIdStr);
      throw new ExecutorManagerException(ne);
    }

    logger.info("Execution ID : " + execId);

    // AZ_HOME must provide a correct path, if not then azHome is set to current working dir.
    final String azHome = Optional.ofNullable(System.getenv(AZ_HOME)).orElse("");

    final Path currentDir = Paths.get(azHome).toAbsolutePath();

    // Set Azkaban props
    final Path jobtypePluginPath = Paths.get(currentDir.toString(), JOBTYPE_DIR);
    Props azkabanProps = setAzkabanProps(jobtypePluginPath);

    // Setup Injector
    setInjector(azkabanProps);

    // Constructor
    final FlowContainer flowContainer = SERVICE_PROVIDER.getInstance(FlowContainer.class);
    launchCtrlMsgListener(flowContainer);

    // TODO : Revisit this logic with full implementation for JMXBEanManager and other callback mechanisms
    JmxJobMBeanManager.getInstance().initialize(azkabanProps);
    // execute the flow
    flowContainer.submitFlow(execId, currentDir, projectZipName);
  }

  private void setupWorkDir(final Path currentDir, final String projectZipName)
          throws ExecutorManagerException {
    // Move files to respective dirs
    // Create project dir
    final Path projectDirPath = Paths.get(currentDir.toString(), PROJECT_DIR);
    logger.info("Creating project dir");
    try {
      Files.createDirectory(projectDirPath);
    } catch (final IOException e) {
      logger.error("Error creating directory :" + projectDirPath, e);
      throw new ExecutorManagerException(e);
    }
    Path projectZipPath = Paths.get(projectDirPath.toString(), projectZipName);
    logger.info("Moving projectDir:" + projectZipName + ": to " +
            projectZipPath);


    try {
      Files.move(Paths.get(projectZipName), projectZipPath);
    } catch (final IOException e) {
      logger.error("Error moving projectzip to " + projectDirPath, e);
      throw new ExecutorManagerException(e);
    }
    // Unzip the project zip
    FlowContainer.unzipFile(projectZipPath.toString(), projectDirPath);
    this.execDirPath = projectDirPath.toString();
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


  private static void setInjector(final Props azkabanProps){
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
  private void submitFlow(final int execId, final Path currentDir, final String projectZipName)
          throws ExecutorManagerException {
    final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(execId);
    if (flow == null) {
      logger.error("Error loading flow with execution Id " + execId);
      throw new ExecutorManagerException("Error loading flow for exec: " + execId +
              ". Terminating flow container launch");
    }

    // Update the status of the flow from DISPATCHING to PREPARING
    this.flow = flow;
    flow.setStatus(Status.PREPARING);
    this.executorLoader.updateExecutableFlow(flow);

    // setup WorkDir
    setupWorkDir(currentDir, projectZipName);
    this.flowRunner = createFlowRunner(flow);
    submitFlowRunner(this.flowRunner);
  }

  /**
   * create Flow Runner
   * @param flow Executable flow object.
   * @return FlowRunner object.
   * @throws ExecutorManagerException
   */
  private FlowRunner createFlowRunner(final ExecutableFlow flow) throws ExecutorManagerException {
    // set the execution dir
    //TODO : May leverage FlowPreparer for this when project management logic is added.
    flow.setExecutionPath(this.execDirPath);

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
    final FlowRunner flowRunner = new FlowRunner(flow, this.executorLoader,
        this.projectLoader, this.jobTypeManager, this.azKabanProps, eventReporter,
            null, commonMetrics, execMetrics);
    flowRunner.setFlowWatcher(watcher)
        .setJobLogSettings(this.jobLogChunkSize, this.jobLogNumFiles)
        .setValidateProxyUser(this.validateProxyUser)
        .setNumJobThreads(this.numJobThreadPerFlow);

    return flowRunner;
  }

  /**
   * Submits the flow to executorService for execution.
   * @param flowRunner The FlowRunner object.
   */
  private void submitFlowRunner(final FlowRunner flowRunner) throws ExecutorManagerException {
    // set running flow, put it in DB
    logger.info("Submitting flow with execution Id " + flowRunner.getExecutionId());
    final Future<?> flowFuture = this.executorService.submit(flowRunner);
    try {
      flowFuture.get();
    } catch (final InterruptedException | ExecutionException e) {
      logger.error(ExceptionUtils.getStackTrace(e));
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * Unzip a file.
   * @param zipPath The source zip file.
   * @param dirPath The destination of the zip file content.
   * @throws ExecutorManagerException
   */
  private static void unzipFile(final String zipPath,
      final Path dirPath)  throws ExecutorManagerException {
    ZipFile zipFile;
    File unzipped;
    try {
      zipFile = new ZipFile(zipPath);
      logger.info("Source path : " + zipPath);
      unzipped = new File(dirPath.toString());
      logger.info("Unzipped file dir : " + unzipped.toString());
    } catch (final IOException e) {
      logger.error("Error creating Zipfile object for zipPath : " + zipPath, e);
      throw new ExecutorManagerException(e);
    }
    try {
      Utils.unzip(zipFile, unzipped);
    } catch (final IOException e) {
      logger.error("Error unzipping zipFile to unzipped : " + unzipped, e);
      throw new ExecutorManagerException(e);
    }
  }

  /**
   * Setup in-memory keystore to be reused for all the job executions in the flow.
   * @throws IOException
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
      // Delete the cert file from disk as the KeyStore is already cached above.
      final File certFile = new File(Constants.ConfigurationKeys.CSR_KEYSTORE_LOCATION);
      if (certFile.delete()) {
        logger.info("Successfully deleted the cert file");
      } else {
        logger.error("Failed to delete the cert file");
        throw new ExecutorManagerException("Failed to delete the cert file");
      }
    }
  }

  public void cancelFlow(final int execId, final String user)
    throws ExecutorManagerException {

    logger.info("Cancel Flow called");
    if (this.flowRunner == null) {
      throw new ExecutorManagerException("Flow has not launched yet.");
    }

    if (Status.isStatusFinished(this.flowRunner.getExecutableFlow().getStatus())) {
      logger.warn("Found a finished execution in the list of running flows: " + execId);
      throw new ExecutorManagerException("Execution is already finished.");
    }

    this.flowRunner.kill(user);
  }

  public LogData readFlowLogs(final int execId, final int startByte, final int length)
    throws ExecutorManagerException {
    logger.info("readFlowLogs called");
    if (this.flowRunner == null) {
      throw new ExecutorManagerException("The flow has not launched yet!");
    }

    final File dir = flowRunner.getExecutionDir();
    if (dir != null && dir.exists()) {
      try {
        final File logFile = flowRunner.getFlowLogFile();
        if (logFile != null && logFile.exists()) {
          return FileIOUtils.readUtf8File(logFile, startByte, length);
        } else {
          throw new ExecutorManagerException("Flow log file does not exist.");
        }
      } catch (final IOException e) {
        throw new ExecutorManagerException(e);
      }
    }

    throw new ExecutorManagerException(
        "Error reading file. Execution directory does not exist");
  }

  public LogData readJobLogs(final int execId, final String jobId, final int attempt,
      final int startByte, final int length) throws ExecutorManagerException {

    logger.info("readJobLogs called");
    if (this.flowRunner == null) {
      throw new ExecutorManagerException("The flow has not launched yet!");
    }

    final File dir = flowRunner.getExecutionDir();
    if (dir != null && dir.exists()) {
      try {
        final File logFile = flowRunner.getJobLogFile(jobId, attempt);
        if (logFile != null && logFile.exists()) {
          return FileIOUtils.readUtf8File(logFile, startByte, length);
        } else {
          throw new ExecutorManagerException("Job log file does not exist.");
        }
      } catch (final IOException e) {
        throw new ExecutorManagerException(e);
      }
    }

    throw new ExecutorManagerException(
        "Error reading file. Execution directory does not exist.");
  }

  public JobMetaData readJobMetaData(final int execId, final String jobId,
      final int attempt, final int startByte, final int length) throws ExecutorManagerException {

    logger.info("readJobMetaData called");
    if (this.flowRunner == null) {
      throw new ExecutorManagerException("The flow has not launched yet.");
    }

    final File dir = flowRunner.getExecutionDir();
    if (dir != null && dir.exists()) {
      try {
        final File metaDataFile = flowRunner.getJobMetaDataFile(jobId, attempt);
        if (metaDataFile != null && metaDataFile.exists()) {
          return FileIOUtils.readUtf8MetaDataFile(metaDataFile, startByte, length);
        } else {
          throw new ExecutorManagerException("Job metadata file does not exist.");
        }
      } catch (final IOException e) {
        throw new ExecutorManagerException(e);
      }
    }
    throw new ExecutorManagerException(
        "Error reading file. Execution directory does not exist.");
  }

  private static void launchCtrlMsgListener(FlowContainer flowContainer) {
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
}
