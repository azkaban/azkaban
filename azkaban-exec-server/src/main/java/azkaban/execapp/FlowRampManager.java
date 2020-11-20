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
package azkaban.execapp;

import static azkaban.ServiceProvider.SERVICE_PROVIDER;

import azkaban.Constants;
import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableFlowRampMetadata;
import azkaban.executor.ExecutableRamp;
import azkaban.executor.ExecutableRamp.Action;
import azkaban.executor.ExecutableRampDependencyMap;
import azkaban.executor.ExecutableRampExceptionalFlowItemsMap;
import azkaban.executor.ExecutableRampExceptionalItems;
import azkaban.executor.ExecutableRampExceptionalJobItemsMap;
import azkaban.executor.ExecutableRampItemsMap;
import azkaban.executor.ExecutableRampMap;
import azkaban.executor.ExecutableRampStatus;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.ramppolicy.RampPolicy;
import azkaban.ramppolicy.RampPolicyManager;
import azkaban.spi.EventType;
import azkaban.utils.FileIOUtils;
import azkaban.utils.OsCpuUtil;
import azkaban.utils.Props;
import azkaban.utils.SystemMemoryInfo;
import azkaban.utils.ThreadPoolExecutingListener;
import azkaban.utils.TimeUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Flow Ramp Manager
 */
@Singleton
public class FlowRampManager implements EventListener<Event>, ThreadPoolExecutingListener {

  private static final String JAR_DEPENDENCY_PREFIX = "jar:";
  private static String LIB_JAR_REG_EXP_FORMATTER = "^(%s)-\\d.*(.jar)$";
  private static String ALL_LIB_JAR_REG_EXP = "^.*(.jar)$";
  private static String LIB_SUB_FOLDER_NAME = "lib";
  private static String EXCLUDED_SUB_FOLDER_NAME = "excluded";
  private static String EXCLUDED_LIB_SUB_FOLDER_NAME = "excluded/lib";

  private static final Logger LOGGER = LoggerFactory.getLogger(FlowRampManager.class);

  private final boolean isRampFeatureEnabled;
  private final boolean isRampPollingServiceEnabled;
  private final int statusPollingIntervalMinutes;
  private final int statusPushIntervalMax;
  private final int statusPullIntervalMax;
  private final RampPolicyManager rampPolicyManager;

  private ExecutorLoader executorLoader;
  private Props azkabanProps;
  private Props globalProps;
  private PollingService pollingService = null;

  // Hosting All Active Ramps, Map.Key is rampId
  private volatile ExecutableRampMap executableRampMap = null;

  // Hosting All Ramp Items, Map.Key is rampId
  private volatile ExecutableRampItemsMap executableRampItemsMap = null;

  // Hosting All Default Value of dependencies, Map.Key is dependencyId
  private volatile ExecutableRampDependencyMap executableRampDependencyMap = null;

  // Hosting Flow Level Special Treatment List, Map.Key is rampId
  private volatile ExecutableRampExceptionalFlowItemsMap executableRampExceptionalFlowItemsMap = null;

  // Hosting Job Level Special Treatment List, Map.Key is RampId + FlowId
  private volatile ExecutableRampExceptionalJobItemsMap executableRampExceptionalJobItemsMap = null;

  private volatile RampDataModel rampDataModel = new RampDataModel();

  //  private volatile boolean active;
  private volatile long latestDataBaseSynchronizationTimeStamp = 0;

  @Inject
  public FlowRampManager(final Props props, final ExecutorLoader executorLoader) throws IOException {
    this.executorLoader = executorLoader;
    this.azkabanProps = props;

    // Check ramp.feature.enabled azkaban setting for backward compatible
    isRampFeatureEnabled = this.azkabanProps.getBoolean(
        Constants.ConfigurationKeys.AZKABAN_RAMP_ENABLED,
        Constants.DEFAULT_AZKABAN_RAMP_ENABLED);
    statusPushIntervalMax = this.azkabanProps.getInt(
        Constants.ConfigurationKeys.AZKABAN_RAMP_STATUS_PUSH_INTERVAL_MAX,
        Constants.DEFAULT_AZKABAN_RAMP_STATUS_PUSH_INTERVAL_MAX);
    statusPullIntervalMax = this.azkabanProps.getInt(
        Constants.ConfigurationKeys.AZKABAN_RAMP_STATUS_PULL_INTERVAL_MAX,
        Constants.DEFAULT_AZKABAN_RAMP_STATUS_PULL_INTERVAL_MAX);
    isRampPollingServiceEnabled = this.azkabanProps.getBoolean(
        Constants.ConfigurationKeys.AZKABAN_RAMP_STATUS_POLLING_ENABLED,
        Constants.DEFAULT_AZKABAN_RAMP_STATUS_POOLING_ENABLED);
    statusPollingIntervalMinutes = this.azkabanProps.getInt(
        Constants.ConfigurationKeys.AZKABAN_RAMP_STATUS_POLLING_INTERVAL,
        Constants.DEFAULT_AZKABAN_RAMP_STATUS_POLLING_INTERVAL);

    if (isRampFeatureEnabled) {

      // load global Props
      String globalPropertiesExtPath = props.getString(
          Constants.ConfigurationKeys.AZKABAN_GLOBAL_PROPERTIES_EXT_PATH, null);
      this.globalProps = globalPropertiesExtPath == null ? null : new Props(null, globalPropertiesExtPath);

      this.rampPolicyManager = new RampPolicyManager(
          props.getString(AzkabanExecutorServer.RAMPPOLICY_PLUGIN_DIR, Constants.PluginManager.RAMPPOLICY_DEFAULTDIR),
          this.globalProps, getClass().getClassLoader());

      this.executableRampMap = ExecutableRampMap.createInstance();
      this.executableRampItemsMap = ExecutableRampItemsMap.createInstance();
      this.executableRampDependencyMap = ExecutableRampDependencyMap.createInstance();
      this.executableRampExceptionalFlowItemsMap = ExecutableRampExceptionalFlowItemsMap.createInstance();
      this.executableRampExceptionalJobItemsMap = ExecutableRampExceptionalJobItemsMap.createInstance();
      this.rampDataModel = new RampDataModel();

      //Load current Ramp Setting from DB
      loadSettings();

      // Start Polling Service to synchronize ramp status cross multiple ExecServer if it is necessary
      if (isRampPollingServiceEnabled) {
        this.LOGGER.info("Starting polling service.");
        this.pollingService = new FlowRampManager.PollingService(this.statusPollingIntervalMinutes,
            new FlowRampManager.PollingCriteria(this.azkabanProps, this.rampDataModel));
        this.pollingService.start();
      }
    } else {
      this.rampPolicyManager = null;
    }
  }

  /**
   * Check if the system is activating the ramp feature, aka some system configuration is ramping.
   */
  private boolean isRampFeatureActivated() {
    if (isRampFeatureEnabled || executableRampMap != null) {
      if (!executableRampMap.getActivatedAll().isEmpty()) {
        return true;
      }
    }
    return false;
  }


  /**
   * Ramp Feature need to track the health of running flow to determine if stop/pause Ramp will be necessary.
   * FlowRampManager is registered into the FlowRunnerManager's Listeners
   */
  @Override
  public void handleEvent(Event event) {
    if (!isRampFeatureActivated()) return;

    if (event.getType() == EventType.FLOW_STARTED || event.getType() == EventType.FLOW_FINISHED) {
      final FlowRunner flowRunner = (FlowRunner) event.getRunner();
      logFlowEvent(flowRunner, event.getType());
    }
  }

  @Override
  public void beforeExecute(Runnable r) {
  }

  @Override
  public void afterExecute(Runnable r) {

  }

  /**
   * This shuts down the flow ramp. The call is blocking and awaits execution of all jobs.
   */
  public void shutdown() {
    if (isRampPollingServiceEnabled) {
      LOGGER.warn("Shutting down FlowRampManager...");
      pollingService.shutdown();
      // Persistent cached data into DB
      saveSettings();
      LOGGER.warn("Shutdown FlowRampManager complete.");
    }
  }

  /**
   * This attempts shuts down the flow runner immediately (unsafe). This doesn't wait for jobs to
   * finish but interrupts all threads.
   */
  public void shutdownNow() {
    if (isRampPollingServiceEnabled) {
      LOGGER.warn("Shutting down FlowRampManager now...");
      pollingService.shutdown();
    }
  }

  public int getNumOfRamps() {
    return executableRampMap.getActivatedAll().size();
  }

  /**
   * Load all ramp Settings from DB
   */
  @VisibleForTesting
  synchronized void loadSettings() {
    loadExecutableRamps();
    loadExecutableRampItems();
    loadExecutableRampDependencies();
    loadExecutableRampExceptionalFlowItems();
    loadExecutableRampExceptionalJobItems();
    latestDataBaseSynchronizationTimeStamp = System.currentTimeMillis();
    LOGGER.info(String.format("Ramp Settings had been successfully loaded at [%d].",
        latestDataBaseSynchronizationTimeStamp));
  }

  /**
   * Load All active ramps, Key = rampId
   */
  @VisibleForTesting
  synchronized void loadExecutableRamps() {
    try {
      if (executableRampMap == null) {
        executableRampMap = executorLoader.fetchExecutableRampMap();
      } else {
        executableRampMap.refresh(executorLoader.fetchExecutableRampMap());
      }
    } catch (ExecutorManagerException e) {
      LOGGER.error("Load all active Executable Ramp failure");
    }
  }

  /**
   * Load All dependency properties into the executableRampProperties Map, Key = rampId,
   */
  @VisibleForTesting
  synchronized void loadExecutableRampItems() {
    try {
      if (executableRampItemsMap == null) {
        executableRampItemsMap = executorLoader.fetchExecutableRampItemsMap();
      } else {
        executableRampItemsMap.refresh(executorLoader.fetchExecutableRampItemsMap());
      }
    } catch (ExecutorManagerException e) {
      LOGGER.error("Load Executable Ramp Items failure");
    }
  }

  /**
   * Load All Default dependency values for ramp
   * When the dependency does not have ramp setting, the default Value will be applied.
   */
  @VisibleForTesting
  synchronized void loadExecutableRampDependencies() {
    try {
      if (executableRampDependencyMap == null) {
        executableRampDependencyMap = executorLoader.fetchExecutableRampDependencyMap();
      } else {
        executableRampDependencyMap.refresh(executorLoader.fetchExecutableRampDependencyMap());
      }
    } catch (ExecutorManagerException e) {
      LOGGER.error("Load Executable Ramp Dependencies failure");
    }
  }

  /**
   * Load All Ramp Exceptional Items on Flow Level
   */
  @VisibleForTesting
  synchronized void loadExecutableRampExceptionalFlowItems() {
    try {
      if (executableRampExceptionalFlowItemsMap == null) {
        executableRampExceptionalFlowItemsMap = executorLoader.fetchExecutableRampExceptionalFlowItemsMap();
      } else {
        executableRampExceptionalFlowItemsMap.refresh(executorLoader.fetchExecutableRampExceptionalFlowItemsMap());
      }
    } catch (ExecutorManagerException e) {
      LOGGER.error("Load Executable Ramp Exceptional Items on Flow Level Failure");
    }
  }

  /**
   * Load All Ramp Exceptional Items on Job Level
   */
  @VisibleForTesting
  synchronized void loadExecutableRampExceptionalJobItems() {
    try {
      if (executableRampExceptionalJobItemsMap == null) {
        executableRampExceptionalJobItemsMap = executorLoader.fetchExecutableRampExceptionalJobItemsMap();
      } else {
        executableRampExceptionalJobItemsMap.refresh(executorLoader.fetchExecutableRampExceptionalJobItemsMap());
      }
    } catch (ExecutorManagerException e) {
      LOGGER.error("Load Executable Ramp Exceptional Items on Job Level Failure");
    }
  }

  /**
   * Save all ramp settings into DB
   */
  @VisibleForTesting
  synchronized void saveSettings() {
    executableRampMap
        .getAll()
        .stream()
        .filter(ExecutableRamp::isChanged)
        .forEach(this::updateExecutableRamp);
    executableRampExceptionalFlowItemsMap
        .entrySet()
        .stream()
        .forEach(this::updateExecutedRampFlows);
    rampDataModel.resetFlowCountAfterSave();
    LOGGER.info("Ramp Settings had been successfully saved.");
  }

  @VisibleForTesting
  /**
   * Persistent all Executable Ramp Status in this azkaban executor into the DB
   */
  synchronized void updateExecutableRamp(ExecutableRamp executableRamp) {
    try {
      // Save all cachedNumTrail, cachedNumSuccess, cachedNumFailure, cachedNumIgnored,
      // save isPaused, endTime when it is not zero, lastUpdatedTime when it is changed.
      executorLoader.updateExecutableRamp(executableRamp);
      // mark cache has been saved
      executableRamp.cacheSaved();
    } catch (ExecutorManagerException e) {
      LOGGER.error(String.format("Update Executable Ramp [%s] Failure.", executableRamp.getId()));
    }
  }

  /**
   * Save All Ramp Exceptional Items on Flow Level
   */
  @VisibleForTesting
  synchronized void updateExecutedRampFlows(Map.Entry<String, ExecutableRampExceptionalItems> entry) {
    try {
      // Save all Identified workflow into the DB
      executorLoader.updateExecutedRampFlows(entry.getKey(), entry.getValue());
    } catch (ExecutorManagerException e) {
      LOGGER.error("Fail to append ramp items into DB.", e);
    }
  }

  /**
   * Call to set Executable Ramp Metadata into ExecutableFlow
   */
  synchronized public void configure(ExecutableFlow executableFlow, File flowDirectory) {

    if (!isRampFeatureActivated()) return;

    // To be safe, check if there is any jar files in ./excluded folder
    // and move them back to the place in original location of the package
    moveFiles(
        FileIOUtils.getDirectory(flowDirectory, EXCLUDED_SUB_FOLDER_NAME),
        flowDirectory,
        ALL_LIB_JAR_REG_EXP
    );
    moveFiles(
        FileIOUtils.getDirectory(flowDirectory, EXCLUDED_LIB_SUB_FOLDER_NAME),
        FileIOUtils.getDirectory(flowDirectory, LIB_SUB_FOLDER_NAME),
        ALL_LIB_JAR_REG_EXP
    );

    String flowName = executableFlow.getFlowName();

    ExecutableFlowRampMetadata executableFlowRampMetadata =
        ExecutableFlowRampMetadata.createInstance(
            executableRampDependencyMap,
            executableRampExceptionalJobItemsMap.getExceptionalJobItemsByFlow(flowName)
        );

    for (ExecutableRamp executableRamp : executableRampMap.getActivatedAll()) {
      try {
        String rampId = executableRamp.getId();
        LOGGER.info("RAMP_CHECK: (rampId = {}, rampStage = {}, executionId = {}, flowName = {}, RampPercentageId = {})",
            rampId,
            executableRamp.getStage(),
            executableFlow.getExecutionId(),
            flowName,
            executableFlow.getRampPercentageId()
        );

        // get Base Props
        Props baseProps = new Props();
        baseProps.putAll(executableRampDependencyMap.getDefaultValues(executableRampItemsMap.getDependencies(rampId)));

        ExecutableRampStatus status = executableRampExceptionalFlowItemsMap.check(rampId, flowName);
        LOGGER.info("RAMP_STATUS: (Status = {}, flowName = {})", status.name(), flowName);
        switch (status) {
          case BLACKLISTED: // blacklist
            executableFlowRampMetadata.setRampProps(
                rampId,
                Props.getInstance(
                    Props.clone(executableRampItemsMap.getRampItems(rampId)),
                    baseProps,
                    ExecutableRampStatus.BLACKLISTED.name()
                )
            );
            LOGGER.info("RAMP_BLACKLISTED: (rampId = {}, flowName = {})", rampId, flowName);
            break;

          case WHITELISTED: // whitelist
            executableFlowRampMetadata.setRampProps(
                rampId,
                Props.getInstance(
                    baseProps,
                    Props.clone(executableRampItemsMap.getRampItems(rampId)),
                    ExecutableRampStatus.WHITELISTED.name()
                )
            );
            LOGGER.info("RAMP_WHITELISTED: (rampId = {}, flowName = {})", rampId, flowName);
            break;

          case SELECTED: // selected
            executableFlowRampMetadata.setRampProps(
                rampId,
                Props.getInstance(
                    baseProps,
                    Props.clone(executableRampItemsMap.getRampItems(rampId)),
                    ExecutableRampStatus.SELECTED.name()
                )
            );
            LOGGER.info("RAMP_SELECTED: (rampId = {}, flowName = {})", rampId, flowName);
            break;

          case UNSELECTED: // selected
            executableFlowRampMetadata.setRampProps(
                rampId,
                Props.getInstance(
                    Props.clone(executableRampItemsMap.getRampItems(rampId)),
                    baseProps,
                    ExecutableRampStatus.UNSELECTED.name()
                )
            );
            LOGGER.info("RAMP_UNSELECTED: (rampId = {}, flowName = {})", rampId, flowName);
            break;

          case EXCLUDED:
            executableFlowRampMetadata.setRampProps(
                rampId,
                Props.getInstance(
                    null,
                    baseProps,
                    ExecutableRampStatus.EXCLUDED.name()
                )
            );
            LOGGER.info("RAMP_EXECLUDED: (rampId = {}, flowName = {})", rampId, flowName);
            break;

          default:
            RampPolicy rampPolicy = rampPolicyManager.buildRampPolicyExecutor(executableRamp.getPolicy(), globalProps);
            LOGGER.info ("RAMP_POLICY_SELECTING: (policy = {}, rampId = {}, flowName = {}, executionId = {}, RampPercentageId = {})",
                rampPolicy.getClass().getName(),
                rampId,
                flowName,
                executableFlow.getExecutionId(),
                executableFlow.getRampPercentageId()
            );
            if (rampPolicy.check(executableFlow, executableRamp)) {
              // Ramp Enabled
              executableFlowRampMetadata.setRampProps(
                  rampId,
                  Props.getInstance(
                      baseProps,
                      Props.clone(executableRampItemsMap.getRampItems(rampId)),
                      ExecutableRampStatus.SELECTED.name()
                  )
              );
              LOGGER.info("RAMP_POLICY_SELECTED: (rampId = {}, flowName = {})", rampId, flowName);
            } else {
              executableFlowRampMetadata.setRampProps(
                  rampId,
                  Props.getInstance(
                      Props.clone(executableRampItemsMap.getRampItems(rampId)),
                      baseProps,
                      ExecutableRampStatus.UNSELECTED.name()
                  )
              );
              LOGGER.info("RAMP_POLICY_UNSELECTED: (rampId = {}, flowName = {})", rampId, flowName);
            }
            break;
        }

        // Remove Package Dependencies
        List<String> removableDependencies = executableRampItemsMap
            .getDependencies(rampId)
            .stream()
            .filter(key -> key.startsWith(JAR_DEPENDENCY_PREFIX))
            .filter(key -> (!baseProps.get(key).isEmpty() || !executableFlowRampMetadata.getRampItemValue(rampId, key).isEmpty()))
            .map(key -> key.substring(JAR_DEPENDENCY_PREFIX.length()))
            .collect(Collectors.toList());
        String regExpression = String.format(LIB_JAR_REG_EXP_FORMATTER, String.join("|", removableDependencies));

        if (!removableDependencies.isEmpty()) {
           // Move those selected jar dependencies in ./ and ./lib folders
           // into the ./excluded and ./excluded/lib folder
           moveFiles(
               flowDirectory,
               FileIOUtils.getDirectory(flowDirectory, EXCLUDED_SUB_FOLDER_NAME),
               regExpression
           );
           moveFiles(
               FileIOUtils.getDirectory(flowDirectory, LIB_SUB_FOLDER_NAME),
               FileIOUtils.getDirectory(flowDirectory, EXCLUDED_LIB_SUB_FOLDER_NAME),
               regExpression
           );
        }

      } catch (Exception e) {
        LOGGER.error("RAMP_EXEC_ERROR: (message = {})", e.getMessage());
      }
    }

    // Append the result into the executable flow
    executableFlow.setExecutableFlowRampMetadata(executableFlowRampMetadata);
  }

  private void moveFiles(File sourceDir, File destinationDir, String regExpression) {
    try {
      FileIOUtils.moveFiles(sourceDir, destinationDir, regExpression);
      LOGGER.info("Success to move files from {} to {} with REGEXP {}",
          sourceDir.getAbsolutePath(),
          destinationDir.getAbsolutePath(),
          regExpression);
    } catch (IOException e) {
      LOGGER.error(
          String.format("Fail to move files from %s to %s with REGEXP %s",
              sourceDir.getAbsolutePath(), destinationDir.getAbsolutePath(), regExpression
          ), e);
    }
  }

  synchronized private void logFlowEvent(FlowRunner flowRunner, EventType eventType) {
    final ExecutableFlow flow = flowRunner.getExecutableFlow();
    LOGGER.info("RAMP_FLOW_EVENT_CAPTURED: (ID = {}, FlowName = {}, ExecutionId = {}, FlowStatus = {})",
        flow.getId(),
        flow.getFlowName(),
        flow.getExecutionId(),
        flow.getStatus().toString());
    if (eventType == EventType.FLOW_STARTED) {
      Set<String> activeRamps = flow.getExecutableFlowRampMetadata().getActiveRamps();
      rampDataModel.beginFlow(flow.getExecutionId(), activeRamps);
      LOGGER.info("RAMP_STARTED: (FlowName = {}, ExecutionId = {}, Ramps = {})",
          flow.getFlowName(),
          flow.getExecutionId(), activeRamps.toString());
      if (isDatabasePullingActionRequired()) {
        LOGGER.info("BEGIN Reload ramp settings from DB ......");
        loadSettings();
        LOGGER.info("END Reload ramp settings from DB ......");
      }
    } else { // EventType.FLOW_FINISHED
      logFlowAction(flowRunner, convertToAction(flow.getStatus()));
      Set<String> ramps = rampDataModel.endFlow(flow.getExecutionId());
      LOGGER.info("RAMP_FINISHED: (FlowName = {}, ExecutionId = {}, Ramps = {})",
          flow.getFlowName(),
          flow.getExecutionId(), ramps.toString());

      if (isDatabasePushingActionRequired()) {
        LOGGER.info("BEGIN Save ramp settings into DB ......");
        saveSettings();
        LOGGER.info("END Save ramp settings into DB ......");
      }
    }
  }

  synchronized private void logFlowAction(FlowRunner flowRunner, Action action) {
    flowRunner.getExecutableFlow()
        .getExecutableFlowRampMetadata()
        .getActiveRamps()
        .stream()
        .map(executableRampMap::get)
        .forEach(executableRamp -> {

          LOGGER.info("FlowRunner Save Result after Ramp. [rampId = {}, action = {}]",
              executableRamp.getId(), action.name());
          executableRamp.cacheResult(action);

          if (Action.FAILED.equals(action)) {
            String rampId = executableRamp.getId();
            String flowName =  flowRunner.getExecutableFlow().getFlowName();
            LOGGER.warn("RAMP_EXCLUDE_FLOW: [executionId = {}, rampId = {}, flowName = {}, action = {}, ramp = {}]",
                flowRunner.getExecutableFlow().getExecutionId(),
                rampId,
                flowName,
                action.name(),
                flowRunner.isRamping()
            );

            if (!executableRamp.ignoreTestFailure()) {
              executableRampExceptionalFlowItemsMap.add(rampId, flowName, ExecutableRampStatus.EXCLUDED,
                  System.currentTimeMillis(), true);
            }
          }
        });
  }

  // This check function is only applied on non-polling mode
  synchronized private boolean isDatabasePushingActionRequired() {
    return ((!isRampPollingServiceEnabled) && (statusPushIntervalMax <= rampDataModel.getEndFlowCount()));
  }

  // This check function is only applied on non-polling mode
  synchronized private boolean isDatabasePullingActionRequired() {
    return ((!isRampPollingServiceEnabled) && (statusPullIntervalMax <= rampDataModel.getBeginFlowCount()));
  }

  synchronized private Action convertToAction(Status status) {
    if (Status.FAILED.equals(status)) return Action.FAILED;
    if (Status.isStatusSucceeded(status)) return Action.SUCCEEDED;
    return Action.IGNORED;
  }

  @VisibleForTesting
  static class RampDataModel {
    // Host the current processing ramp flows
    // Map.key = Any ID that uniquely identifies each execution. (we will use executionId). Map.value = Set of ramps
    private volatile Map<Integer, Set<String>> executingFlows = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private volatile int beginFlowCount = 0;
    private volatile int endFlowCount = 0;


    public RampDataModel() {
    }

    public synchronized void beginFlow(final int executionId, Set<String> ramps) {
      lock.lock();
      executingFlows.put(executionId, ramps);
      beginFlowCount++;
      lock.unlock();
    }

    public synchronized Set<String> endFlow(final int executionId) {
      Set<String> ramps = executingFlows.get(executionId);
      lock.lock();
      executingFlows.remove(executionId);
      endFlowCount++;
      lock.unlock();
      return ramps;
    }

    public Map<Integer, Set<String>> getExecutingFlows() {
      return this.executingFlows;
    }

    public int getBeginFlowCount() {
      return beginFlowCount;
    }

    public int getEndFlowCount() {
      return endFlowCount;
    }

    public void resetFlowCountAfterSave() {
      lock.lock();
      beginFlowCount = executingFlows.size();
      endFlowCount = 0;
      lock.unlock();
    }

    public boolean hasUnsavedFinishedFlow() {
      return endFlowCount > 0;
    }
  }

  /**
   * Polls new executions from DB periodically and submits the executions to run on the executor.
   *
   * Polling Service, here, will periodically persistent and reload the diff of the staging ramping status/configurations.
   * It is the key mechanism to communicate the ramp status cross multiple Azkaban ExecServer.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private class PollingService {

    private final ScheduledExecutorService scheduler;
    private final FlowRampManager.PollingCriteria pollingCriteria;
    private final int statusPollingIntervalMinutes;

    public PollingService(final int statusPollingIntervalMinutes, final FlowRampManager.PollingCriteria pollingCriteria) {
      this.statusPollingIntervalMinutes = statusPollingIntervalMinutes;
      this.scheduler = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setNameFormat("azk-flowramp-sched").build());
      this.pollingCriteria = pollingCriteria;
    }

    public void start() {
      this.scheduler.scheduleAtFixedRate(() -> pollExecution(), 0L, this.statusPollingIntervalMinutes,
          TimeUnit.MINUTES);
    }

    private void pollExecution() {
      if (this.pollingCriteria.shouldPoll()) {
        if (this.pollingCriteria.satisfiesUnsavedDataAvailableCriteria()) {
          LOGGER.info("Save Ramp Setting to Database.");
          FlowRampManager.this.saveSettings();
        }
        LOGGER.info("Load Ramp Setting from Database.");
        FlowRampManager.this.loadSettings();
      }
    }

    public void shutdown() {
      this.scheduler.shutdown();
      this.scheduler.shutdownNow();
    }
  }

  private class PollingCriteria {

    private final Props azkabanProps;
    private final RampDataModel rampDataModel;

    private final SystemMemoryInfo memInfo = SERVICE_PROVIDER.getInstance(SystemMemoryInfo.class);
    private final OsCpuUtil cpuUtil = SERVICE_PROVIDER.getInstance(OsCpuUtil.class);

    //    private boolean areFlowThreadsAvailable;
    private boolean isFreeMemoryAvailable;
    private boolean isCpuLoadUnderMax;

    public PollingCriteria(final Props azkabanProps, final RampDataModel rampDataModel) {
      this.azkabanProps = azkabanProps;
      this.rampDataModel = rampDataModel;
    }

    public boolean shouldPoll() {
      return (satisfiesFreeMemoryCriteria() && satisfiesCpuUtilizationCriteria() && satisfiesTimeIntervalCriteria());
    }

    private boolean satisfiesUnsavedDataAvailableCriteria() {
      return this.rampDataModel.hasUnsavedFinishedFlow();
    }

    private boolean satisfiesTimeIntervalCriteria() {
      // To avoid too frequently load, especially avoid the load from polling after initialization
      return TimeUtils.timeEscapedOver(FlowRampManager.this.latestDataBaseSynchronizationTimeStamp, 50);
    }

    private boolean satisfiesFreeMemoryCriteria() {
      final int minFreeMemoryConfigGb = this.azkabanProps.
          getInt(Constants.ConfigurationKeys.AZKABAN_RAMP_STATUS_POLLING_MEMORY_MIN, 0);

      // allow polling if not present or configured with invalid value
      if (minFreeMemoryConfigGb > 0) {
        final int minFreeMemoryConfigKb = minFreeMemoryConfigGb * 1024 * 1024;
        final boolean haveEnoughMemory = this.memInfo.isFreePhysicalMemoryAbove(minFreeMemoryConfigKb);
        if (this.isFreeMemoryAvailable != haveEnoughMemory) {
          this.isFreeMemoryAvailable = haveEnoughMemory;
          if (haveEnoughMemory) {
            FlowRampManager.LOGGER.info("Polling criteria satisfied: available free memory.");
          } else {
            FlowRampManager.LOGGER.info("Polling criteria NOT satisfied: available free memory.");
          }
        }
        return haveEnoughMemory;
      }
      return true;
    }

    private boolean satisfiesCpuUtilizationCriteria() {
      final double maxCpuUtilizationConfig = this.azkabanProps.
          getDouble(Constants.ConfigurationKeys.AZKABAN_RAMP_STATUS_POLLING_CPU_MAX, 100);

      if (maxCpuUtilizationConfig > 0 && maxCpuUtilizationConfig < 100) {
        final double cpuLoad = this.cpuUtil.getCpuLoad();
        if (cpuLoad != -1) {
          final boolean cpuLoadWithinParams = cpuLoad < maxCpuUtilizationConfig;
          if (this.isCpuLoadUnderMax != cpuLoadWithinParams) {
            this.isCpuLoadUnderMax = cpuLoadWithinParams;
            if (cpuLoadWithinParams) {
              FlowRampManager.LOGGER.info("Polling criteria satisfied: Cpu utilization (" + cpuLoad + "%).");
            } else {
              FlowRampManager.LOGGER.info("Polling criteria NOT satisfied: Cpu utilization (" + cpuLoad + "%).");
            }
          }
          return cpuLoadWithinParams;
        }
      }
      return true;
    }
  }
}
