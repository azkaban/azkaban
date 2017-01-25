package azkaban.execapp.cluster;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.cluster.emr.EmrUtils;
import azkaban.execapp.cluster.emr.InvalidEmrConfigurationException;
import azkaban.executor.*;
import azkaban.utils.Props;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryUtils;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static azkaban.execapp.cluster.emr.EmrUtils.*;
import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION;

/**
 * Created by jsoumet on 2/7/16 for azkaban.
 */
public class EmrClusterManager implements IClusterManager, EventListener {

    private static final String EMR_CONF_ENV_NAME = "emr.env.name";
    private static final String EMR_CONF_ENABLED = "cluster.emr.enabled";
    private static final String EMR_CONF_TERMINATE_ERROR = "cluster.emr.terminate.error";
    private static final String EMR_CONF_TERMINATE_COMPLETION = "cluster.emr.terminate.completion";
    private static final String EMR_CONF_SELECT_ID = "cluster.emr.select.id";
    private static final String EMR_CONF_NAME_STRATEGY = "cluster.emr.name.strategy"; // see ClusterNameStrategy enum for possible values
    private static final String EMR_CONF_SPOOLUP_TIMEOUT = "cluster.emr.spoolup.timeout";
    private static final String EMR_CONF_APP_CONFIGURATION_S3_PATH = "cluster.emr.app.configuration.s3.path";
    private static final String EMR_DEFAULT_APP_CONFIGURATION_S3_PATH = "s3://usw2-relateiq-emr-scripts/scripts/hadoo-emr-settings.json";
    // internal flow props
    private static final String EMR_INTERNAL_CLUSTERID = "cluster.emr.internal.clusterid";
    private static final String EMR_INTERNAL_CLUSTERNAME = "cluster.emr.internal.clustername";
    private static final String EMR_INTERNAL_EXECUTION_MODE = "cluster.emr.execution.mode";
    private Logger classLogger = Logger.getLogger(EmrClusterManager.class);
    private Props globalProps = null;
    private ExecutorLoader executorLoader = null;

    private AmazonElasticMapReduceClient emrClient = null;

    private static final long EMR_CLIENT_LIFETIME = 15 * 60 * 1000; // 15 minutes
    private long emrClientTimestamp = 0;

    // Cluster Name -> Integer (Count of flows using cluster) - only going into shutdown process for emr clusters when all executions that use the cluster are done
    private final Map<String, Integer> clusterFlows = new ConcurrentHashMap<String, Integer>();

    // Cluster Name -> Cluster Id - this is used so we can keep the list of running cluster ids to be looked up by cluster name to avoid calls on EMR api (rate limit)
    private final Map<String, String> runningClusters = new ConcurrentHashMap<String, String>();

    // Cluster Id -> Cluster Master Node Ip - track IP address of each cluster's master node to avoid calls to EMR api (rate limit)
    private final Map<String, String> clusterMasterIps = new ConcurrentHashMap<String, String>();

    // (Item is Cluster Id) - List of clusters to keep alive - when multiple flows share the same cluster, if any of the flows indicate that the cluster should not be shutdown (because of an error or any other reason), the cluster should not be terminated even if it's ok to be terminated by the other flows using it.
    private final List<String> clustersKeepAlive = Collections.synchronizedList(new ArrayList<String>());

    private final Map<String, Boolean> clusterStatusCache = ExpiringMap.builder()
            .expiration(30, TimeUnit.SECONDS)
            .entryLoader((String clusterId) -> isClusterReady(getEmrClient(), clusterId))
            .build();

    private static final int MAX_CLIENT_RETRIES = 16;

    public EmrClusterManager(Props serverProps) {
        classLogger.info("Initialized " + this.getClass().getName());
    }

    /**
     * If we are creating a cluster, define how we are gonna name the cluster (the name is important because that's how we reuse the cluster between jobs)
     *
     * @param combinedProps
     * @param logger
     * @return the strategy to be used
     */
    ClusterNameStrategy getClusterNameStrategy(Props combinedProps, Logger logger) {
        ClusterNameStrategy defaultValue = ClusterNameStrategy.EXECUTION_ID;
        String defaultProperty = null;
        String value = combinedProps.getString(EMR_CONF_NAME_STRATEGY, defaultProperty);

        if (value == null) {
            logger.info(EMR_CONF_NAME_STRATEGY + " is not set so defaulting to " + defaultValue);
            return defaultValue;
        }

        try {
            ClusterNameStrategy s = ClusterNameStrategy.valueOf(value.toUpperCase());
            logger.info(EMR_CONF_NAME_STRATEGY + " is set to: " + s);
            return s;

        } catch (Throwable error) {
            logger.warn(EMR_CONF_NAME_STRATEGY + " is set to " + value + " but that's not valid, returning default value: " + defaultValue + ". The possible values are: " + Arrays.toString(ClusterNameStrategy.values()));
            return defaultValue;
        }
    }

    ExecutionMode getFlowExecutionMode(Props combinedProps, Logger logger) {
        if (combinedProps.get(EMR_CONF_SELECT_ID) != null) {
            String clusterId = combinedProps.getString(EMR_CONF_SELECT_ID);
            logger.info(EMR_CONF_SELECT_ID + " was set to " + clusterId + ". Trying to use specific cluster to run this flow.");
            boolean clusterExists = isClusterActive(getEmrClient(), clusterId);
            logger.info("Cluster " + clusterId + " is " + (clusterExists ? "ACTIVE" : "INACTIVE"));
            if (clusterExists) return ExecutionMode.SPECIFIC_CLUSTER;
        }

        if (combinedProps.getBoolean(EMR_CONF_ENABLED, false)) {
            logger.info(EMR_CONF_ENABLED + " is true, will create a dedicated cluster for this flow.");
            return ExecutionMode.CREATE_CLUSTER;
        }

        logger.info(EMR_CONF_ENABLED + " was false or not set, and " + EMR_CONF_SELECT_ID + " was not specified. Will run on default cluster.");
        return ExecutionMode.DEFAULT_CLUSTER;
    }

    String getClusterName(ExecutableFlow flow, Props props, Logger logger) {
        ClusterNameStrategy strategy = getClusterNameStrategy(props, logger);

        if (strategy.equals(ClusterNameStrategy.EXECUTION_ID)) {
            return "Azkaban - Transient Cluster - [" + props.get(EMR_CONF_ENV_NAME) + "-" + flow.getFlowId() + ":" + flow.getExecutionId() + "]";
        } else if (strategy.equals(ClusterNameStrategy.PROJECT_NAME)) {
            return "Azkaban - Transient Cluster - [" + props.get(EMR_CONF_ENV_NAME) + "-" + flow.getProjectName() + "]";
        } else {
            throw new RuntimeException("Don't know how to handle cluster name strategy: " + strategy);
        }
    }

    @Override
    public void handleEvent(Event event) {
        Logger logger = null;
        try {
            if (event.getRunner() instanceof FlowRunner) {
                FlowRunner runner = (FlowRunner) event.getRunner();
                ExecutableFlow flow = runner.getExecutableFlow();
                logger = Logger.getLogger(flow.getExecutionId() + "." + flow.getFlowId());
                logger.info(this.getClass().getName() + " handling " + event.getType() + " " + event.getTime());

                switch (event.getType()) {
                    case FLOW_STARTED:
                        try {
                            boolean runStatus = createClusterAndConfigureJob(flow, logger);
                            updateFlow(flow);
                            if (!runStatus) runner.kill(this.getClass().getName());
                        } catch (Exception e) {
                            logger.error(e);
                            runner.kill(this.getClass().getName());
                        }
                        break;

                    case FLOW_FINISHED:
                        maybeTerminateCluster(flow, logger);
                        break;
                }
            }
        } catch (Throwable error) {
            error.printStackTrace();
            if (logger != null) logger.error(error);
        }
    }

    @Override
    public boolean createClusterAndConfigureJob(ExecutableFlow flow, Logger logger) {
        Logger jobLogger = logger != null ? logger : classLogger;
        Props combinedProps = getCombinedProps(flow);

        jobLogger.info("ClusterManager is configuring job " + flow.getFlowId() + ":" + flow.getExecutionId() + ".");

        printClusterProperties(combinedProps, jobLogger);

        ExecutionMode executionMode = getFlowExecutionMode(combinedProps, jobLogger);
        setClusterProperty(flow, EMR_INTERNAL_EXECUTION_MODE, executionMode.toString());

        int spoolUpTimeoutInMinutes = combinedProps.getInt(EMR_CONF_SPOOLUP_TIMEOUT, 25);
        String clusterId = null;
        String clusterName = null;
        Optional<String> masterIp = Optional.empty();

        switch (executionMode) {
            case SPECIFIC_CLUSTER:
                clusterId = combinedProps.getString(EMR_CONF_SELECT_ID);
                masterIp = getMasterIPCached(clusterId);
                if (masterIp.isPresent()) {
                    setFlowMasterIp(flow, masterIp.get(), jobLogger);
                }
                break;

            case CREATE_CLUSTER:
                try {
                    flow.setStatus(Status.CREATING_CLUSTER);
                    updateFlow(flow);

                    clusterName = getClusterName(flow, combinedProps, logger);

                    // If blockUntilReadyAndReturnMasterIp times out, we should retry with a new cluster.
                    Integer clusterAttempt = 0;
                    Integer totalClusterAttempts = 3;
                    while (clusterAttempt++ < totalClusterAttempts && (clusterId == null || !masterIp.isPresent())){
                        jobLogger.info("Creating or getting cluster with name: " + clusterName + " and obtaining ip (attempt " + clusterAttempt + "/" + totalClusterAttempts + ")");

                        clusterId = getOrCreateClusterByName(flow, jobLogger, combinedProps, clusterName);

                        if (clusterId != null) {
                            jobLogger.info("Getting ip for cluster with id: " + clusterId);
                            masterIp = blockUntilReadyAndReturnMasterIp(clusterId, spoolUpTimeoutInMinutes, jobLogger);

                            if(!masterIp.isPresent()) {
                                jobLogger.error("Timed out waiting " + spoolUpTimeoutInMinutes + " minutes for cluster to start. Shutting down cluster " + clusterId);
                                shutdownCluster(clusterName, clusterId, jobLogger);
                            } else {
                                jobLogger.info("Obtained ip for cluster " + clusterId);
                                break;
                            }
                        }
                    }

                    // By now if we don't have a cluster id, I give up my friend
                    if (clusterId != null && masterIp.isPresent()) {
                        setFlowMasterIp(flow, masterIp.get(), jobLogger);
                        updateFlow(flow);

                    } else {
                        jobLogger.error("Failed all attempts to find and/or start cluster: " + clusterName);

                        shutdownCluster(clusterName, clusterId, jobLogger);
                        updateFlow(flow);
                        return false;
                    }

                } catch (Throwable e) {
                    jobLogger.error(e);
                    e.printStackTrace();
                }

                break;

            case DEFAULT_CLUSTER:
            default:
                // TODO: find default cluster from conf
                // right now.. do nothing

                break;
        }

        // We should have a cluster by now so lets get this puppy going
        if (clusterId != null) {
            setClusterProperty(flow, EMR_INTERNAL_CLUSTERID, clusterId);

            jobLogger.info("Cluster Summary - Id: " + clusterId);

            if (masterIp.isPresent()) {
                jobLogger.info("Cluster Summary - IP: " + masterIp.get());
                jobLogger.info("Cluster Summary - Hadoop Master: http://" + masterIp.get() + ":8088/cluster/apps");
            }
        }

        try {
            updateFlow(flow);
        } catch (ExecutorManagerException e) {
            jobLogger.error("Failed to update flow.");
        }

        jobLogger.info("Cluster configuration complete.");

        return true;
    }

    private String getOrCreateClusterByName(ExecutableFlow flow, Logger jobLogger, Props combinedProps, String clusterName) {
        String clusterId = null;
        // This is to keep track the number of flows that's attached to an EMR cluster
        Integer count = clusterFlows.compute(clusterName, (k, v) -> (v == null) ? 1 : v + 1);
        jobLogger.info("Number of flow(s) currently using cluster " + clusterName + ": " + count);

        setClusterProperty(flow, EMR_INTERNAL_CLUSTERNAME, clusterName);

        // Try to find an existing running cluster with the same name
        Integer lookupAttempt = 0;
        Integer lookupTotalAttempts = 5;
        synchronized (clusterFlows) {
            while (lookupAttempt++ < lookupTotalAttempts) {
                try {
                    jobLogger.info("Trying to find running cluster: " + clusterName + " (Attempt " + lookupAttempt + "/" + lookupTotalAttempts + ")");

                    // Try to find an existing running cluster that's cached (on the first flow, always make sure to actually call EMR's api to get the list of running clusters to minimize the chance of any problem)
                    if (count > 1) {
                        clusterId = runningClusters.get(clusterName);
                        if (clusterId != null) {
                            jobLogger.info("Found existing cached running cluster (" + clusterName + "): " + clusterId);
                            break;
                        }
                    }

                    ClusterSummary runningCluster = EmrUtils.findClusterByName(getEmrClient(), clusterName, EmrUtils.RUNNING_STATES);

                    if (runningCluster != null) {
                        jobLogger.info("Found cluster " + clusterName + " - Id: " + runningCluster.getId() + ", Status: " + runningCluster.getStatus());
                        clusterId = runningCluster.getId();
                        runningClusters.put(clusterName, clusterId);
                        break;
                    } else {
                        jobLogger.info("Couldn't find running cluster " + clusterName + " (Attempt " + lookupAttempt + "/" + lookupTotalAttempts + ")");
                    }

                    // If this is not the first flow attached to the cluster we should really be able to find it, so we are gonna sleep to give the first flow attached to the cluster the chance to call AWS and everything
                    if (count > 1 && lookupAttempt < lookupTotalAttempts - 1) {
                        jobLogger.info("Sleeping for 30 secs....");
                        Thread.sleep(30000);
                        jobLogger.info("Awake!");
                    }

                } catch (Throwable error) {
                    jobLogger.info("Couldn't find running cluster " + clusterName + " (Attempt " + lookupAttempt + "/" + lookupTotalAttempts + ") - Error: " + error);
                }
            }

            // If by now we haven't found a cluster to run this in, let's create a new one
            if (clusterId == null) {
                jobLogger.info("Since we couldn't find a running cluster " + clusterName + ", we are going to create a new one!");

                Integer createAttempt = 0;
                Integer createTotalAttempts = 2;
                while (createAttempt++ < createTotalAttempts) {
                    try {
                        clusterId = createCluster(clusterName, combinedProps, jobLogger);
                        jobLogger.info("Created cluster (Attempt " + createAttempt + "/" + createTotalAttempts + ")");
                        break;

                    } catch (Throwable error) {
                        jobLogger.info("Couldn't create cluster (Attempt " + createAttempt + "/" + createTotalAttempts + "): " + clusterName + " - Error: " + error);
                    }
                }
            }
        }
        return clusterId;
    }

    boolean shutdownCluster(String clusterName, String clusterId, Logger logger) {
        logger.info("Terminating Cluster - Id: " + clusterId + ", Name: " + clusterName);

        try {
            return EmrUtils.terminateCluster(getEmrClient(), clusterId);

        } catch (Throwable error) {
            logger.error("Error terminating cluster (id: " + clusterId + ", name: " + clusterName + "): " + error);
            return false;

        } finally {
            terminateCleanup(clusterId, clusterName, logger);
        }
    }

    boolean shouldShutdown(Props combinedProps, Status flowStatus, Logger logger) {
        boolean terminateOnCompletion = combinedProps.getBoolean(EMR_CONF_TERMINATE_COMPLETION, true);
        boolean terminateOnError = combinedProps.getBoolean(EMR_CONF_TERMINATE_ERROR, true);

        switch (flowStatus) {
            case SUCCEEDED:
                logger.info("Flow succeeded and " + EMR_CONF_TERMINATE_COMPLETION + "=" + terminateOnCompletion);
                return terminateOnCompletion;
            case FAILED:
                logger.info("Flow failed and " + EMR_CONF_TERMINATE_ERROR + "=" + terminateOnError);
                return terminateOnError;
            case KILLED:
                logger.info("Flow was killed and " + EMR_CONF_TERMINATE_COMPLETION + "=" + terminateOnCompletion);
                return terminateOnCompletion;
            default:
                logger.info("Flow completed with unknown status: " + flowStatus + " and " + EMR_CONF_TERMINATE_COMPLETION + "=" + terminateOnCompletion);
                return terminateOnCompletion;
        }
    }

    @Override
    public void maybeTerminateCluster(ExecutableFlow flow, Logger logger) {
        Logger jobLogger = logger != null ? logger : classLogger;
        Props combinedProps = getCombinedProps(flow);
        ExecutionMode executionMode = ExecutionMode.valueOf(combinedProps.get(EMR_INTERNAL_EXECUTION_MODE));

        switch (executionMode) {
            case CREATE_CLUSTER:
                maybeTerminateEphemeralCluster(flow, combinedProps, jobLogger);
                break;

            default:
                jobLogger.info("Execution mode was " + executionMode + ", so cluster will not be terminated.");
                break;
        }
    }

    /**
     * This is the method that's called that will maybe terminate ephemoral emr clusters
     * If more than one flow shares a cluster, we will only shutdown the cluster if all of the flows are ok for that to happen (shouldShutdown() returning true for each flow instance)
     *
     * @param flow
     * @param combinedProps
     * @param jobLogger
     */
    private synchronized void maybeTerminateEphemeralCluster(ExecutableFlow flow, Props combinedProps, Logger jobLogger) {
        // Flow Properties
        Status flowStatus = flow.getStatus();

        // Cluster Properties
        String clusterId = combinedProps.get(EMR_INTERNAL_CLUSTERID);
        String clusterName = combinedProps.get(EMR_INTERNAL_CLUSTERNAME);

        jobLogger.info("Starting shutdown process for cluster (" + clusterId + "): " + clusterName);

        if (clusterId == null) {
            jobLogger.warn("Cluster id was null, not terminating cluster!");
            return;
        }

        try {
            // Is this flow only ok to shutdown the cluster?
            boolean okForThisFlowToShutdownCluster = shouldShutdown(combinedProps, flow.getStatus(), jobLogger);

            // Count down all the flows are using the cluster so shutdown only happens when all flows are done
            Integer count = clusterFlows.computeIfPresent(clusterName, (key, value) -> {
                Integer current = value - 1;
                if (current <= 0) {
                    return null;
                } else {
                    return current;
                }
            });

            if (count == null) count = 0;
            boolean lastFlowUsingCluster = count <= 0;

            // Are the other flows sharing the cluster ok to shutdown the cluster?
            Boolean otherFlowsOkToShutdown = !clustersKeepAlive.contains(clusterId);

            // Log Debug
            jobLogger.info("Shutdown Process - Flow Status: " + flowStatus);
            jobLogger.info("Shutdown Process - Cluster Id: " + clusterId);
            jobLogger.info("Shutdown Process - Cluster Name: " + clusterName);
            jobLogger.info("Shutdown Process - Latch Count: " + count);
            jobLogger.info("Shutdown Process - Is ok with this flow to shutdown cluster: " + okForThisFlowToShutdownCluster);

            if (lastFlowUsingCluster) {
                jobLogger.info("Shutdown Process - Is ok with the other flows sharing this cluster to shutdown the cluster: " + otherFlowsOkToShutdown);
            } else {
                jobLogger.info("Looks there are still other flows using cluster " + clusterId + " so we are not gonna shut it down");
                return;
            }

            if (okForThisFlowToShutdownCluster && otherFlowsOkToShutdown) {
                shutdownCluster(clusterName, clusterId, jobLogger);

            } else if (!okForThisFlowToShutdownCluster) {
                // If we are not shutting down the cluster now and it's not ok for the cluster to be shutdown from this specific flow's prespective, add note to that
                jobLogger.info("Not ok for with this flow for this cluster to be shutdown, keeping track in case this cluster gets reused by another flow...");
                clustersKeepAlive.add(clusterId);

            } else if (!otherFlowsOkToShutdown) {
                // Not ok to shutdown with some other flow that used this cluster
                jobLogger.warn("We are not shutting down cluster " + clusterId + " because it's not ok with one of the flows that used the same cluster");

            }

        } catch (Throwable error) {
            jobLogger.error("Error during shutdown process - Cluster Id: " + clusterId + ", Cluster Name: " + clusterName + ", Error: " + error.getMessage());
            error.printStackTrace();
        }
    }

    public void terminateCleanup(String clusterId, String clusterName, Logger logger) {
        logger.info("Cleaning up state and cache for  cluster " + clusterName + " (" + clusterId + ")");

        // If we don't remove this, there wouldn't be any problems, maybe some memory leak
        // If we remove this too early, we might miss the fact an earlier workflow requested this cluster to not be shutdown
        clustersKeepAlive.remove(clusterId);

        // If we don't remove this, the cluster by this name would never shutdown (when flows share cluster name)
        // If we remove this too early, we wouldn't know there are other workflows currently using this cluster
        clusterFlows.remove(clusterName);

        // If we don't remove this, cached entry would point to potentially an invalid cluster id, but retry would fix the issue
        // This is just a cache for EMR
        runningClusters.remove(clusterName);

        if (clusterId != null) {
            clusterMasterIps.remove(clusterId);
        }
    }

    private String createCluster(String clusterName, Props combinedProps, Logger jobLogger) throws InvalidEmrConfigurationException, IOException {
        jobLogger.info("Preparing new EMR cluster request to run this flow. Cluster name will be " + clusterName + ".");

        JobFlowInstancesConfig instancesConfig = createEMRClusterInstanceConfig(combinedProps);
        String jsonConfigUrl = combinedProps.getString(EMR_CONF_APP_CONFIGURATION_S3_PATH, EMR_DEFAULT_APP_CONFIGURATION_S3_PATH);
        Collection<Configuration> clusterConfigurations = getClusterJSONConfiguration(getS3Client(), jsonConfigUrl);
        RunJobFlowRequest emrClusterRequest = createRunJobFlowRequest(combinedProps, clusterName, instancesConfig, clusterConfigurations);

        jobLogger.info("Cluster configuration for " + clusterName + " passed validation.");
        jobLogger.info("Initiating new cluster request on EMR.");

        RunJobFlowResult result = getEmrClient().runJobFlow(emrClusterRequest);

        String clusterId = result.getJobFlowId();
        jobLogger.info("New cluster created with id: " + clusterId);

        runningClusters.put(clusterName, clusterId);

        return clusterId;
    }

    public Optional<String> blockUntilReadyAndReturnMasterIp(String clusterId, int timeoutInMinutes, Logger
            jobLogger) {
        if (clusterId == null) return Optional.empty();

        jobLogger.info("Will wait for cluster to be ready for " + timeoutInMinutes + " minutes (" + EMR_CONF_SPOOLUP_TIMEOUT + ")");
        try {
            int tempTimeoutInMinutes = timeoutInMinutes;
            while (--tempTimeoutInMinutes > 0) {
                if (clusterStatusCache.get(clusterId)) {
                    Optional<String> masterIp = getMasterIPCached(clusterId);
                    if (masterIp.isPresent()) {
                        jobLogger.info("Cluster " + clusterId + " is ready! Spinup time was " + (timeoutInMinutes - tempTimeoutInMinutes) + " minutes.");
                        return masterIp;
                    } else {
                        jobLogger.info("Cluster " + clusterId + " is ready but failed to find master IP.");
                        return Optional.empty();
                    }
                }
                jobLogger.info("Waiting for cluster " + clusterId + " to be ready... " + tempTimeoutInMinutes + " minutes remaining.");
                Thread.sleep(60000);
            }
        } catch (InterruptedException e) {
            jobLogger.error(e);
        }
        jobLogger.error("Cluster failed to spool up after waiting for " + timeoutInMinutes + " mins.");
        return Optional.empty();
    }

    public Optional<String> getMasterIPCached(String clusterId) {
        if (clusterMasterIps.containsKey(clusterId)) {
            return Optional.of(clusterMasterIps.get(clusterId));
        }

        Optional<String> masterIp = getMasterIP(getEmrClient(), clusterId);
        if (masterIp.isPresent()) {
            clusterMasterIps.put(clusterId, masterIp.get());
        }

        return masterIp;
    }

    private void updateFlow(ExecutableFlow flow) throws ExecutorManagerException {
        flow.setUpdateTime(System.currentTimeMillis());
        if (executorLoader == null) executorLoader = AzkabanExecutorServer.getApp().getExecutorLoader();
        executorLoader.updateExecutableFlow(flow);
    }

    Props getCombinedProps(ExecutableFlow flow) {
        ExecutionOptions executionOptions = flow.getExecutionOptions();
        HashMap<String, Object> clusterProperties = executionOptions.getClusterProperties();
        if (clusterProperties == null) {
            clusterProperties = new HashMap<>();
            executionOptions.setClusterProperties(clusterProperties);
        }
        if (globalProps == null) {
            globalProps = AzkabanExecutorServer.getApp().getFlowRunnerManager().getGlobalProps();
        }
        Props combinedProps = new Props(propsFromMap(clusterProperties), globalProps);
        flow.getInputProps().getMapByPrefix("cluster.").forEach((key, val) -> combinedProps.put("cluster." + key, val));
        return combinedProps;
    }

    private void setClusterProperty(ExecutableFlow flow, String key, Object value) {
        ExecutionOptions executionOptions = flow.getExecutionOptions();
        HashMap<String, Object> clusterProperties = executionOptions.getClusterProperties();
        if (clusterProperties == null) {
            clusterProperties = new HashMap<>();
            executionOptions.setClusterProperties(clusterProperties);
        }
        clusterProperties.put(key, value);
    }

    public void setFlowMasterIp(ExecutableFlow flow, String masterIp, Logger jobLogger) {
        jobLogger.info("Updating hadoop master ip with " + masterIp);
        flow.getInputProps().put("hadoop-inject.hadoop.master.ip", masterIp);
    }

    private Props propsFromMap(Map<String, Object> map) {
        Props newProps = new Props();
        for (String key : map.keySet()) {
            Object val = map.get(key);
            if (val instanceof String) newProps.put(key, (String) val);
            if (val instanceof Integer) newProps.put(key, (Integer) val);
            if (val instanceof Long) newProps.put(key, (Long) val);
            if (val instanceof Double) newProps.put(key, (Double) val);
        }
        return newProps;
    }

    private void printClusterProperties(Props combinedProps, Logger jobLogger) {
        {
            jobLogger.info("Execution options:");
            combinedProps.getKeySet()
                    .stream()
                    .filter(k -> k.startsWith("cluster."))
                    .forEach(key -> jobLogger.info(key + "=" + combinedProps.get(key)));
        }
    }

    private AWSCredentials getAWSCredentials() {
        DefaultAWSCredentialsProviderChain providerChain = new DefaultAWSCredentialsProviderChain();
        return providerChain.getCredentials();
    }

    private AmazonS3Client getS3Client() {
        return new AmazonS3Client(getAWSCredentials()).withRegion(Regions.US_WEST_2);
    }

    private AmazonElasticMapReduceClient getEmrClient() {
        if (emrClient == null || System.currentTimeMillis() - emrClientTimestamp > EMR_CLIENT_LIFETIME) {
            RetryPolicy retryPolicy = new RetryPolicy(DEFAULT_RETRY_CONDITION,
                    new IncreasedThrottleBackoffStrategy(), MAX_CLIENT_RETRIES, true);
            emrClient = new AmazonElasticMapReduceClient(getAWSCredentials(),
                    PredefinedClientConfigurations.defaultConfig().withRetryPolicy(retryPolicy))
                    .withRegion(Regions.US_WEST_2);
            emrClientTimestamp = System.currentTimeMillis();
        }

        return emrClient;
    }

    private enum ExecutionMode {
        DEFAULT_CLUSTER,
        CREATE_CLUSTER,
        SPECIFIC_CLUSTER
    }

    private enum ClusterNameStrategy {
        EXECUTION_ID,
        PROJECT_NAME;
    }

    private static class IncreasedThrottleBackoffStrategy implements RetryPolicy.BackoffStrategy {

        /** Base sleep time (milliseconds) for general exceptions. **/
        private static final int SCALE_FACTOR = 300;

        /** Base sleep time (milliseconds) for throttling exceptions. **/
        private static final int THROTTLING_SCALE_FACTOR = 1000;

        private static final int THROTTLING_SCALE_FACTOR_RANDOM_RANGE = THROTTLING_SCALE_FACTOR / 4;

        /** Maximum exponential back-off time before retrying a request */
        private static final int MAX_BACKOFF_IN_MILLISECONDS = 120 * 1000;

        /**
         * Maximum number of retries before the max backoff will be hit. This is
         * calculated via log_2(MAX_BACKOFF_IN_MILLISECONDS / SCALE_FACTOR)
         * based on the code below.
         */
        private static final int MAX_RETRIES_BEFORE_MAX_BACKOFF = 8;

        /** For generating a random scale factor **/
        private final Random random = new Random();

        /** {@inheritDoc} */
        @Override
        public final long delayBeforeNextRetry(AmazonWebServiceRequest originalRequest,
                                               AmazonClientException exception,
                                               int retriesAttempted) {
            if (retriesAttempted < 0) return 0;
            if (retriesAttempted > MAX_RETRIES_BEFORE_MAX_BACKOFF) return MAX_BACKOFF_IN_MILLISECONDS;

            int scaleFactor;
            if (exception instanceof AmazonServiceException
                    && RetryUtils.isThrottlingException((AmazonServiceException)exception)) {
                scaleFactor = THROTTLING_SCALE_FACTOR + random.nextInt(THROTTLING_SCALE_FACTOR_RANDOM_RANGE);
            } else {
                scaleFactor = SCALE_FACTOR;
            }

            long delay = (1L << retriesAttempted) * scaleFactor;
            delay = Math.min(delay, MAX_BACKOFF_IN_MILLISECONDS);

            return delay;
        }
    }
}
