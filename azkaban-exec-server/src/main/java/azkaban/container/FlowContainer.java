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

import azkaban.AzkabanCommonModule;
import azkaban.Constants;
import azkaban.Constants.PluginManager;
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
import azkaban.utils.Props;
import azkaban.utils.StdOutErrRedirect;
import azkaban.utils.Utils;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This class is the entrypoint for launching a flow execution in a container.
 *  It sets up the Azkaban Properties, the DAO, injects all the required classes, sets up the
 *  execution directory along with the project files, creates the FlowRunner and submits it to
 *  the executor service for execution.
 *  It assumes that their is a certain directory structure consisting of all the dependencies.
 *  The dependencies such as Hadoop, Hive, Pig, and other libraries.
 */
public class FlowContainer {

  private static final String PROJECT_DIR = "project";
  private static final String JOBTYPE_DIR = "plugins/jobtypes";
  private static final String CONF_ARG = "-conf";
  private static final String CONF_DIR = "conf";
  private static final String FLOW_NUM_JOB_THREADS = "flow.num.job.threads";
  private static final String DEFAULT_LOG_CHUNK_SIZE = "5MB";
  private static final int DEFAULT_LOG_NUM_FILES = 4;
  private static final int ARG_EXEC_ID = 0;
  private static final int ARG_PROJECT_LOCATION = 1;
  private static final int DEFAULT_FLOW_NUM_JOB_TREADS = 10;
  private static final String AZ_HOME = "AZ_HOME";

  private static final Logger logger = LoggerFactory.getLogger(FlowContainer.class);

  private final ExecutorService executorService;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final JobTypeManager jobTypeManager;
  private final Props azKabanProps;
  private Props globalProps;
  private final int numJobThreadPerFlow;
  private final String execDirPath;

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
  private FlowContainer(final Props props, final String execDirPath)
          throws IOException {

    // Create Azkaban Props Map
    this.azKabanProps = props;
    // Setup global props if applicable
    final String globalPropsPath = this.azKabanProps.getString("executor.global.properties", null);
    if (globalPropsPath != null) {
      this.globalProps = new Props(null, globalPropsPath);
    }

    // execution dir
    this.execDirPath = execDirPath;

    this.executorLoader = SERVICE_PROVIDER.getInstance(JdbcExecutorLoader.class);
    logger.info("executorLoader from guice :" + this.executorLoader);

    // project Loader
    this.projectLoader = SERVICE_PROVIDER.getInstance(JdbcProjectImpl.class);
    logger.info("projectLoader from guice : " + this.projectLoader);

    // setup executor service
    this.executorService = Executors.newSingleThreadExecutor();

    this.jobLogChunkSize = this.azKabanProps.getString("job.log.chunk.size",
            DEFAULT_LOG_CHUNK_SIZE);
    this.jobLogNumFiles = this.azKabanProps.getInt("job.log.backup.index", DEFAULT_LOG_NUM_FILES);
    this.validateProxyUser = this.azKabanProps.getBoolean("proxy.user.lock.down",
            Constants.DEFAULT_VALIDATE_PROXY_USER);
    this.jobTypeManager =
        new JobTypeManager(
            this.azKabanProps.getString(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
                PluginManager.JOBTYPE_DEFAULTDIR),
            this.globalProps, getClass().getClassLoader());

    this.numJobThreadPerFlow = props.getInt(FLOW_NUM_JOB_THREADS, DEFAULT_FLOW_NUM_JOB_TREADS);
    // Setting up the in-memory KeyStore for all the job executions in the flow.
    setupKeyStore();
  }

  /**
   * The entry point of FlowContainer. Validates the input arguments and submits the flow for
   * execution. It is assumed that AZ_HOME environment variable is set. If it is not set, then
   * it explicitely sets it to present working directory.
   * @param args Takes the execution id and Project zip file path as inputs.
   * @throws IOException
   * @throws ExecutorManagerException
   */
  public static void main(final String[] args) throws IOException, ExecutorManagerException {
    // Redirect all std out and err messages into slf4j
    StdOutErrRedirect.redirectOutAndErrToLog();
    // Get all the arguments
    final String execIdStr = args[ARG_EXEC_ID];
    final String projectZipName = args[ARG_PROJECT_LOCATION];
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

    final Path currentDir = Paths.get(System.getenv(AZ_HOME)).toAbsolutePath();

    // Set Azkaban props
    final Path jobtypePluginPath = Paths.get(currentDir.toString(), JOBTYPE_DIR);
    Props azkabanProps = FlowContainer.setAzkabanProps(jobtypePluginPath);

    // Setup Injector
    FlowContainer.setInjector(azkabanProps);

    // Setup work directories
    final String execDirPath = FlowContainer.setupWorkDir(currentDir, projectZipName);

    // Constructor
    final FlowContainer flowContainer =
            new FlowContainer(azkabanProps, execDirPath);

    // TODO : Revisit this logic with full implementation for JMXBEanManager and other callback mechanisms
    JmxJobMBeanManager.getInstance().initialize(azkabanProps);
    // execute the flow
    flowContainer.submitFlow(execId);
  }

  private static String setupWorkDir(final Path currentDir, final String projectZipName)
          throws IOException {
    // Move files to respective dirs
    // Create project dir
    final Path projectDirPath = Paths.get(currentDir.toString(), PROJECT_DIR);
    logger.info("Creating project dir");
    Files.createDirectory(projectDirPath);
    Path projectZipPath = Paths.get(projectDirPath.toString(), projectZipName);
    logger.info("moving projectDir:" + projectZipName + ": to " +
            projectZipPath);

    Files.move(Paths.get(projectZipName), projectZipPath);
    // Unzip the project zip
    FlowContainer.unzipFile(projectZipPath.toString(), projectDirPath);
    return projectDirPath.toString();
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
            new AzkabanCommonModule(azkabanProps)
    );
    SERVICE_PROVIDER.setInjector(injector);
  }

  /**
   * Submit flow
   * Creates and submits the FlowRunner.
   * @param execId Execution Id of the flow.
   * @throws ExecutorManagerException
   */
  private void submitFlow(final int execId) throws ExecutorManagerException {
    submitFlowRunner(createFlowRunner(execId));
  }

  /**
   * create Flow Runner
   * @param execId Execution Id of the flow.
   * @return FlowRunner object.
   * @throws ExecutorManagerException
   */
  private FlowRunner createFlowRunner(final int execId) throws ExecutorManagerException {
    final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(execId);
    if (flow == null) {
      logger.error("Error loading flow with execution Id " + execId);
      throw new ExecutorManagerException("Error loading flow for exec: " + execId +
              ". Terminating flow container launch");
    }

    // Update the status of the flow from DISPATCHING to PREPARING
    flow.setStatus(Status.PREPARING);
    this.executorLoader.updateExecutableFlow(flow);

    // set the execution dir :
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
   * @throws IOException
   */
  private static void unzipFile(final String zipPath,
      final Path dirPath) throws IOException {
    final ZipFile zipFile = new ZipFile(zipPath);
    logger.info("source path = " + zipPath);
    final File unzipped = new File(dirPath.toString());
    logger.info("unzipped file dir = " + unzipped.toString());
    Utils.unzip(zipFile, unzipped);
    zipFile.close();
  }

  /**
   * Setup in-memory keystore to be reused for all the job executions in the flow.
   * @throws IOException
   */
  private void setupKeyStore() throws IOException {
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
        throw new IOException("Failed to Prefetch KeyStore");
      }
      // Delete the cert file from disk as the KeyStore is already cached above.
      final File certFile = new File(Constants.ConfigurationKeys.CSR_KEYSTORE_LOCATION);
      if (certFile.delete()) {
        logger.info("Successfully deleted the cert file");
      } else {
        logger.error("Failed to delete the cert file");
        throw new IOException("Failed to delete the cert file");
      }
    }

  }

}
