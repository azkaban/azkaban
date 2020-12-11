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
import azkaban.db.DBMetrics;
import azkaban.db.DatabaseOperator;
import azkaban.db.MySQLDataSource;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.ExecMetrics;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.event.FlowWatcher;
import azkaban.execapp.event.RemoteFlowWatcher;
import azkaban.executor.ActiveExecutingFlowsDao;
import azkaban.executor.AssignExecutorDao;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionFlowDao;
import azkaban.executor.ExecutionJobDao;
import azkaban.executor.ExecutionLogsDao;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionRampDao;
import azkaban.executor.ExecutorDao;
import azkaban.executor.ExecutorEventsDao;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.FetchActiveFlowDao;
import azkaban.executor.JdbcExecutorLoader;
import azkaban.executor.NumExecutionsDao;
import azkaban.executor.Status;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.jobtype.HadoopProxy;
import azkaban.jobtype.JobTypeManager;
import azkaban.metrics.CommonMetrics;
import azkaban.metrics.MetricsManager;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.ProjectLoader;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.server.AzkabanServer;
import azkaban.spi.AzkabanEventReporter;
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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipFile;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This class is responsible for launching a flow in a container.
 *  For each such flow, a FlowContainer is created which runs in a container during the lifetime
 *  of the said flow. This class is responsible for achieving most of the things done by
 *  FlowRunnerManager, however, since it only needs to handle one flow so the logic is much more
 *  lightweight.
 *  It assumes that their is a certain directory structure consisting of all the dependencies.
 */
public class FlowContainer {

  private static final String PROJECT_DIR = "project";
  private static final String JOBTYPE_DIR = "jobtypes";
  private static final String CONF_ARG = "-conf";
  private static final String CONF_DIR = "conf";

  private static final Logger logger = LoggerFactory.getLogger(FlowContainer.class);

  // FlowRunnerManager specific code
  private final ExecutorService executorService;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final JobTypeManager jobTypeManager;
  private final Props azKabanProps;
  private Props globalProps;

  // We want to limit the log sizes to about 20 megs
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
  private FlowContainer(final Props props)
          throws IOException {

    // Create Azkaban Props Map
    this.azKabanProps = props;
    // Setup global props if applicable
    final String globalPropsPath = this.azKabanProps.getString("executor.global.properties", null);
    if (globalPropsPath != null) {
      this.globalProps = new Props(null, globalPropsPath);
    }

    // Setup DAO, a lot of it is redundant
    final DataSource dataSource= new MySQLDataSource(this.azKabanProps,
        new DBMetrics(new MetricsManager(new MetricRegistry())));

    final DatabaseOperator dbOperator = new DatabaseOperator(
        new QueryRunner(dataSource));

    this.executorLoader = setupDao(dbOperator);

    // project Loader
    this.projectLoader = new JdbcProjectImpl(this.azKabanProps, dbOperator);

    // setup executor service
    this.executorService = Executors.newSingleThreadExecutor();

    this.jobLogChunkSize = this.azKabanProps.getString("job.log.chunk.size", "5MB");
    this.jobLogNumFiles = this.azKabanProps.getInt("job.log.backup.index", 4);
    this.validateProxyUser = this.azKabanProps.getBoolean("proxy.user.lock.down", false);
    this.jobTypeManager =
        new JobTypeManager(
            this.azKabanProps.getString(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
                PluginManager.JOBTYPE_DEFAULTDIR),
            this.globalProps, getClass().getClassLoader());
    // Fetch keyStore props and use it to get the KeyStore, put it in JobTypeManager
    Props keyStoreLoadProps = this.jobTypeManager.getKeyStoreLoadProps();
    if (keyStoreLoadProps != null) {
      // Load HadoopSecurityManager
      HadoopSecurityManager hadoopSecurityManager = null;
      try {
        final String hadoopSecurityClassName =
                keyStoreLoadProps.getString(HadoopJobUtils.HADOOP_SECURITY_MANAGER_CLASS_PARAM);
        final Class<?> hadoopSecurityManagerClass =
                HadoopProxy.class.getClassLoader().loadClass(hadoopSecurityClassName);

        logger.info("Loading hadoop security manager " + hadoopSecurityManagerClass.getName());
        hadoopSecurityManager = (HadoopSecurityManager)
                Utils.callConstructor(hadoopSecurityManagerClass, keyStoreLoadProps);
      } catch (final Exception e) {
        logger.error("Could not instantiate Hadoop Security Manager ", e);
        throw new RuntimeException("Failed to get hadoop security manager!"
                + e.getCause(), e);
      }

      final KeyStore keyStore = hadoopSecurityManager.getKeyStore(keyStoreLoadProps);
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

  /**
   * The entry point of FlowContainer. Validates the input arguments and submits the flow for
   * execution.
   * @param args Takes the execution id and Project zip file path as inputs.
   * @throws IOException
   * @throws ExecutorManagerException
   */
  public static void main(final String[] args) throws IOException, ExecutorManagerException {
    // Redirect all std out and err messages into log4j
    StdOutErrRedirect.redirectOutAndErrToLog();
    // Get all the arguments
    final String execIdStr = args[0];
    final String projectZipName = args[1];
    // Process Execution ID.
    int execId = -1;
    try {
      execId = Integer.parseInt(execIdStr);
    } catch (NumberFormatException ne) {
      logger.error("Execution ID argument is invalid {}", execIdStr);
      throw new ExecutorManagerException(ne);
    }

    logger.info("Execution ID = " + execId);

    // Setup work directories
    final Path currentWorkingDir = Paths.get("").toAbsolutePath();
    final Path projectDirPath = Paths.get(currentWorkingDir.toString(), PROJECT_DIR);
    final Path jobtypePluginPath = Paths.get(currentWorkingDir.toString(), JOBTYPE_DIR);

    // Move files to respective dirs
    // Create project dir
    // TODO: Need to revisit
    logger.info("Creating project dir");
    Files.createDirectory(projectDirPath);
    Path projectZipPath = Paths.get(projectDirPath.toString(), projectZipName);
    logger.info("moving projectDir:" + projectZipName + ": to " +
        projectZipPath);

    Files.move(Paths.get(projectZipName), projectZipPath);
    // TODO : revisit this logic, depending on what jar management provides.
    // Unzip the project zip
    FlowContainer.unzipFile(projectZipPath.toString(), projectDirPath);

    // Set Azkaban props
    Props props = setAzkabanProps(jobtypePluginPath);

    // Inject AzkabanCommonModule
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props)
    );
    SERVICE_PROVIDER.setInjector(injector);

    // Constructor
    final FlowContainer flowContainer =
        new FlowContainer(props);

    // Use some execId to execute the flow
    flowContainer.submitFlow(execId);
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

    // Set db stuff.
    return new Props(props, propsMap);
  }

  /**
   * Submit flow
   * Creates and submits the FlowRunner.
   * @param execId Execution Id of the flow.
   * @throws ExecutorManagerException
   */
  private void submitFlow(final int execId) throws ExecutorManagerException {
    final FlowRunner flowRunner = createFlowRunner(execId);
    submitFlowRunner(flowRunner);
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
      throw new ExecutorManagerException("Error loading flow with execution Id " + execId);
    }

    // Update the status of the flow from DISPATCHING to PREPARING
    flow.setStatus(Status.PREPARING);
    this.executorLoader.updateExecutableFlow(flow);

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
        .setNumJobThreads(20);

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
   * Setup all the DAO needed for a flow execution.
   * @param dbOperator DatabaseOperator
   * @return ExecutorLoader object which interacts with the database.
   */
  private ExecutorLoader setupDao(final DatabaseOperator dbOperator) {
    final ExecutionFlowDao executionFlowDao = new ExecutionFlowDao(dbOperator, null);
    final ExecutorDao executorDao = new ExecutorDao(dbOperator);
    final ExecutionJobDao executionJobDao = new ExecutionJobDao(dbOperator);
    final ExecutionLogsDao executionLogsDao = new ExecutionLogsDao(dbOperator);
    final ExecutorEventsDao executorEventsDao = new ExecutorEventsDao(dbOperator);
    final ActiveExecutingFlowsDao activeExecutingFlowsDao =
        new ActiveExecutingFlowsDao(dbOperator);
    final FetchActiveFlowDao fetchActiveFlowDao =
        new FetchActiveFlowDao(dbOperator);
    final AssignExecutorDao assignExecutorDao =
        new AssignExecutorDao(dbOperator, executorDao);
    final NumExecutionsDao numExecutionsDao = new NumExecutionsDao(dbOperator);
    final ExecutionRampDao executionRampDao = new ExecutionRampDao(dbOperator);

    // Use above objects to create Executor Loader
    return new JdbcExecutorLoader(executionFlowDao, executorDao,
        executionJobDao, executionLogsDao, executorEventsDao,
        activeExecutingFlowsDao, fetchActiveFlowDao, assignExecutorDao,
        numExecutionsDao, executionRampDao);

  }
}
