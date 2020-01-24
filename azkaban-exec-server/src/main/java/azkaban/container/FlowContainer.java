/*
 * Copyright 2019 LinkedIn Corp.
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
import azkaban.Constants.PluginManager;
import azkaban.db.DBMetrics;
import azkaban.db.DatabaseOperator;
import azkaban.db.MySQLDataSource;
import azkaban.execapp.AzkabanExecutorServer;
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
import azkaban.flow.Flow;
import azkaban.flow.FlowUtils;
import azkaban.jobtype.JobTypeManager;
import azkaban.metrics.MetricsManager;
import azkaban.project.JdbcProjectImpl;
import azkaban.project.Project;
import azkaban.project.ProjectLoader;
import azkaban.project.ProjectManagerException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipFile;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.log4j.Logger;

/**
 *  This class is responsible for launching a flow in a container.
 */
public class FlowContainer {

  private static final String PROJECT_DIR = "project";
  private static final String JOBTYPE_DIR = "jobtypes";
  private static final String AZ_DIR = "azkaban_libs";
  //private final Path currentWorkingDir;
  private final File tokenFile;

  private static final Logger logger = Logger.getLogger(FlowContainer.class);

  // FlowRunnerManager specific code
  private final ExecutorService executorService;
  private final ExecutorLoader executorLoader;
  private final ProjectLoader projectLoader;
  private final JobTypeManager jobTypeManager;
  //private final AzkabanEventReporter azkabanEventReporter;
  private final Props azKabanProps;
  private Props globalProps;
  private final File projectDir;

  private YARNFlowRunner flowRunner;
  private Future flowFuture;

  // We want to limit the log sizes to about 20 megs
  private final String jobLogChunkSize;
  private final int jobLogNumFiles;
  // If true, jobs will validate proxy user against a list of valid proxy users.
  private final boolean validateProxyUser;

  private final Project project;

  // TODO: throw away
  private int execId = -1;

  private FlowContainer(final Path projectDirPath,
      final Path tokenFile, final Props props) throws IOException {
    this.projectDir = projectDirPath.toFile();
    this.tokenFile = tokenFile != null ? tokenFile.toFile() : null;

    // Add the token file
    // TODO: Figure out how to use this token file.
    props.put("tokenFile", this.tokenFile != null ? this.tokenFile.toString() : null);

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
    // load project TODO: use parameterized project name
    this.project = this.projectLoader.fetchProjectByName("basic_flow");
    if (this.project == null) {
      System.out.println("Failed to fetch project basic_flow");
    }
    // TODO : throw away work, I guess
    // Load all the flows
    try {
      final List<Flow> flows = this.projectLoader.fetchAllProjectFlows(this.project);
      final Map<String, Flow> flowMap = new HashMap<>();
      for (final Flow flow : flows) {
        flowMap.put(flow.getId(), flow);
      }
      this.project.setFlows(flowMap);
    } catch (final ProjectManagerException e) {
      throw new IOException("Failed to load flow for project " + project.getName());
    }

    // setup executor service, TODO : revisit
    this.executorService = Executors.newSingleThreadExecutor();

    this.jobLogChunkSize = this.azKabanProps.getString("job.log.chunk.size", "5MB");
    this.jobLogNumFiles = this.azKabanProps.getInt("job.log.backup.index", 4);
    this.validateProxyUser = this.azKabanProps.getBoolean("proxy.user.lock.down", false);
    this.jobTypeManager =
        new JobTypeManager(
            this.azKabanProps.getString(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
                PluginManager.JOBTYPE_DEFAULTDIR),
            this.globalProps, getClass().getClassLoader());
  }

  public static void main(final String[] args) throws IOException, ExecutorManagerException {
    // Redirect all std out and err messages into log4j
    StdOutErrRedirect.redirectOutAndErrToLog();
    // Get all the arguments
    final String projectDir = args[0];
    // The jobtypedir is not needed for certain types.
    // it looks like "jobTypeDir=<jobTypejars.zip> else,
    // "jobTypeDir=None"
    // TODO : Add a validation logic.
    final String flowName = args[1];
    final String jobtypeDirArg = args[2];
    final String azLibDir = args[3];
    final int numArgs = args.length;
    String tokenFile = null;
    if (numArgs == 5) {
      // delegation token defined
      tokenFile = args[4];
    }
    final String jobtypeDir = jobtypeDirArg.endsWith("None") ? null :
        jobtypeDirArg.substring(jobtypeDirArg.indexOf("=") + 1);

    // Setup work directories
    final Path currentWorkingDir = Paths.get("").toAbsolutePath();
    final Path projectDirPath = Paths.get(currentWorkingDir.toString(), PROJECT_DIR);
    final Path jobtypePluginPath = Paths.get(currentWorkingDir.toString(), JOBTYPE_DIR);
    Path tokenFilePath = null;
    Path projectZipPath = null;

    // Move files to respective dirs
    // Create project dir
    System.out.println("Creating project dir");
    Files.createDirectory(projectDirPath);
    projectZipPath = Paths.get(projectDirPath.toString(), projectDir);
    System.out.println("moving projectDir:" + projectDir + ": to " +
        projectZipPath);
    Files.move(Paths.get(projectDir), projectZipPath);


    // TODO: reviist the logic for Hive
    /*System.out.println("Setting up Hive Dir");
    final Path hivePath = Paths.get(currentWorkingDir.toString(), "hive");
    Files.createDirectories(hivePath);
    FlowContainer.unzipFile("hive.zip", hivePath);*/

    // Create jobtype dir
    System.out.println("Creating jobtype dir");
    Files.createDirectories(jobtypePluginPath);
    // TODO : revisit this logic,
    // Unzip the project zip
    FlowContainer.unzipFile(projectZipPath.toString(), projectDirPath);
    if (jobtypeDir != null) {
      FlowContainer.unzipFile(jobtypeDir, jobtypePluginPath);
    }

/*    // Common properties
    System.out.println("Moving commonProperties:" + commonProps + ": to " +
        Paths.get(jobtypePluginPath.toString(), commonProps));
    Files.move(Paths.get(commonProps),
        Paths.get(jobtypePluginPath.toString(), commonProps));
    // Common private properties
    System.out.println("Moving commonPrivate:" + commonPrivateProps +
        ": to "
        + Paths.get(jobtypePluginPath.toString(), commonPrivateProps));
    Files.move(Paths.get(commonPrivateProps),
        Paths.get(jobtypePluginPath.toString(), commonPrivateProps));*/

    // Move jobtype zip
    /*Path jobTypeZipPath = null;
    if (jobtypeDir != null) {
      System.out.println("Moving jobtypeDir:" + jobtypeDir + ": to " +
          Paths.get(jobtypePluginPath.toString(), jobType));
      Files.move(Paths.get(jobtypeDir), Paths.get(jobtypePluginPath.toString(), jobType));
      jobTypeZipPath = Paths.get(projectDirPath.toString(), jobtypeDir);
      System.out.println("moving jobtype zip: " + jobtypeDir + " to " +
          jobTypeZipPath);
      Files.move(Paths.get(jobtypeDir), jobTypeZipPath);
    }*/

    // Move Azkaban lib
    System.out.println("Moving azLibDir:" + azLibDir + ": to " +
        Paths.get(currentWorkingDir.toString(), AZ_DIR));
    Files.move(Paths.get(azLibDir), Paths.get(currentWorkingDir.toString(), AZ_DIR));

    // Delegation token
    if (tokenFile != null) {
      tokenFilePath = Paths.get(projectDirPath.toString(), tokenFile);
      System.out.println("Moving delegation token: " + tokenFile + ": to " +
          tokenFilePath);
      Files.move(Paths.get(tokenFile), tokenFilePath);
    }

    // TODO : Deepak - throw away
    Path pwd = Paths.get("").toAbsolutePath();
    System.out.println("Deepak : Dumping dir structure for = " + pwd.toString());
    java.nio.file.Files.walk(pwd).filter(java.nio.file.Files::isRegularFile)
        .forEach(System.out::println);

    // Set Azkaban props
    Props props = setAzkabanProps(jobtypePluginPath);
    // TODO : revisit Guice logic
    final Injector injector = Guice.createInjector(
        new AzkabanCommonModule(props)
    );
    SERVICE_PROVIDER.setInjector(injector);

    // Constructor
    final FlowContainer flowContainer =
        new FlowContainer(projectDirPath, tokenFilePath, props);

    // Use some execId to execute the flow
    flowContainer.submitFlow(flowName);
  }

  // Set Azkaban Props
  private static Props setAzkabanProps(final Path jobtypePluginPath) {
    final Map<String, String> propsMap = new HashMap<>();
    // Set db stuff.
    propsMap.put("database.type", "mysql");
    propsMap.put("mysql.host", "makto-db-079.corp.linkedin.com");
    propsMap.put("mysql.port", "3306");
    propsMap.put("mysql.database", "azkaban_mlearn_a");
    propsMap.put("mysql.user", "az_mlearn_alpha");
    propsMap.put("mysql.password", "j64fg2d");
    propsMap.put("mysql.numconnections", "200");
    propsMap.put(AzkabanExecutorServer.JOBTYPE_PLUGIN_DIR,
        jobtypePluginPath.toString());

    return new Props(null, propsMap);
  }

  // Submit flow
  // TODO : Once mimicDispatch is gone, use execId from container params
  public void submitFlow(final String flowName) throws ExecutorManagerException {
    mimicDispatch(flowName); // throw it out.
    final YARNFlowRunner flowRunner = createFlowRunner(this.execId);
    submitFlowRunner(flowRunner);
  }

  // create Flow Runner
  private YARNFlowRunner createFlowRunner(final int execId) throws ExecutorManagerException {
    final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(execId);
    if (flow == null) {
      throw new ExecutorManagerException("Error loading flow with exec " + execId);
    }

    // Setup flow runner
    FlowWatcher watcher = null;
    final ExecutionOptions options = flow.getExecutionOptions();
    if (options.getPipelineExecutionId() != null) {
      final int pipelinedExecId = options.getPipelineExecutionId();
      watcher = new RemoteFlowWatcher(pipelinedExecId, this.executorLoader);
    }

    final YARNFlowRunner flowRunner = new YARNFlowRunner(flow, this.executorLoader,
        this.projectLoader, this.jobTypeManager, this.azKabanProps, null,
        this.projectDir);

    flowRunner.setFlowWatcher(watcher)
        .setJobLogSettings(this.jobLogChunkSize, this.jobLogNumFiles)
        .setValidateProxyUser(this.validateProxyUser)
        .setNumJobThreads(20);

    return flowRunner;
  }

  private void submitFlowRunner(final YARNFlowRunner flowRunner) {
    // set running flow, put it in DB
    this.flowRunner = flowRunner;
    this.flowFuture = this.executorService.submit(flowRunner);
    try {
      this.flowFuture.get();
    } catch (final InterruptedException ie) {
      ie.printStackTrace();
    } catch (final ExecutionException ee) {
      ee.printStackTrace();
    }
  }

  // TODO : throw away code
  // This code is to mimic dispatch logic.
  private void mimicDispatch(final String flowName) throws ExecutorManagerException {
    final User user = new User("djaiswal");
    System.out.println("project name = " + project.getName());
    Map<String, Flow> flowMap = project.getFlowMap();
    System.out.println("project num flows = " + flowMap.size());
    for (Flow curFlow: flowMap.values()) {
      System.out.println("A flow in project " + project.getName() + " is " + curFlow.getId());
    }
    // Fetch Flow
    final Flow flow = this.project.getFlow(flowName);
    if (flow == null) {
      System.out.println("Failed to fetch flow " + flowName);
      return;
    }
    System.out.println("Flow = " + flow.getId());
    ExecutableFlow executableFlow = FlowUtils.createExecutableFlow(this.project, flow);
    System.out.println("Executable flow = " + executableFlow);
    executableFlow.setSubmitUser(user.getUserId());
    executableFlow.setStatus(Status.PREPARING);
    executableFlow.setSubmitTime(System.currentTimeMillis());
    // upload the flow to db
    this.executorLoader.uploadExecutableFlow(executableFlow);
    // Verify if it is uploaded
    final int execId = executableFlow.getExecutionId();
    System.out.println("execID = " + execId);
    this.execId = execId;
  }

  private static void unzipFile(final String zipPath,
      final Path dirPath) throws IOException {
    final ZipFile zipFile = new ZipFile(zipPath);
    System.out.println("source path = " + zipPath);
    final File unzipped = new File(dirPath.toString());
    System.out.println("unzipped file dir = " + unzipped.toString());
    Utils.unzip(zipFile, unzipped);
    zipFile.close();
  }

  private ExecutorLoader setupDao(final DatabaseOperator dbOperator) {
    final ExecutionFlowDao executionFlowDao = new ExecutionFlowDao(dbOperator);
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
