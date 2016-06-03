package azkaban.execapp.cluster;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.cluster.emr.EmrUtils;
import azkaban.execapp.cluster.emr.InvalidEmrConfigurationException;
import azkaban.executor.*;
import azkaban.utils.Props;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static azkaban.execapp.cluster.emr.EmrUtils.*;

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

    // Cluster Name -> Integer (Count of flows using cluster) - only going into shutdown process for emr clusters when all executions that use the cluster are done
    private Map<String, Integer> clusterFlows = new ConcurrentHashMap<String, Integer>();

    // (Item is Cluster Id) - List of clusters to keep alive - when multiple flows share the same cluster, if any of the flows indicate that the cluster should not be shutdown (because of an error or any other reason), the cluster should not be terminated even if it's ok to be terminated by the other flows using it.
    private List<String> clustersKeepAlive = Collections.synchronizedList(new ArrayList<String>());

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
        if (event.getRunner() instanceof FlowRunner) {
            FlowRunner runner = (FlowRunner) event.getRunner();
            ExecutableFlow flow = runner.getExecutableFlow();
            Logger logger = Logger.getLogger(flow.getExecutionId() + "." + flow.getFlowId());
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
                Optional<String> specificMasterIp = getMasterIP(getEmrClient(), clusterId);
                if (specificMasterIp.isPresent()) {
                    setFlowMasterIp(flow, specificMasterIp.get(), jobLogger);
                }
                break;

            case CREATE_CLUSTER:
                try {
                    flow.setStatus(Status.CREATING_CLUSTER);
                    updateFlow(flow);

                    clusterName = getClusterName(flow, combinedProps, logger);

                    // This is to keep track the number of flows that's attached to an EMR cluster
                    clusterFlows.computeIfPresent(clusterName, (key, value) -> value + 1);
                    clusterFlows.putIfAbsent(clusterName, 1);

                    setClusterProperty(flow, EMR_INTERNAL_CLUSTERNAME, clusterName);

                    // Try to find an existing running cluster with the same name
                    ClusterSummary runningCluster = EmrUtils.findClusterByName(getEmrClient(), clusterName, EmrUtils.RUNNING_STATES);
                    if (runningCluster != null) {
                        clusterId = runningCluster.getId();
                        jobLogger.info("Found existing running cluster (" + clusterName + "): " + runningCluster.getId());
                    }

                    // If by now we haven't found a cluster to run this in, let's create a new one
                    if (clusterId == null) {
                        jobLogger.info("Preparing new EMR cluster request to run this flow. Cluster name will be " + clusterName + ".");
                        JobFlowInstancesConfig instancesConfig = createEMRClusterInstanceConfig(combinedProps);
                        String jsonConfigUrl = combinedProps.getString(EMR_CONF_APP_CONFIGURATION_S3_PATH, EMR_DEFAULT_APP_CONFIGURATION_S3_PATH);
                        Collection<Configuration> clusterConfigurations = getClusterJSONConfiguration(getS3Client(), jsonConfigUrl);
                        RunJobFlowRequest emrClusterRequest = createRunJobFlowRequest(combinedProps, clusterName, instancesConfig, clusterConfigurations);
                        jobLogger.info("Cluster configuration for " + clusterName + " passed validation.");
                        jobLogger.info("Initiating new cluster request on EMR.");
                        RunJobFlowResult result = getEmrClient().runJobFlow(emrClusterRequest);
                        clusterId = result.getJobFlowId();
                        jobLogger.info("New cluster created with id: " + clusterId);
                    }

                    // By now if we don't have a cluster id, I give up my friend
                    if (clusterId == null) {
                        throw new RuntimeException("We couldn't find/create a cluster for this job to run on -- please contact your favorite production engineer <:");
                    }

                    masterIp = blockUntilReadyAndReturnMasterIp(clusterId, spoolUpTimeoutInMinutes, jobLogger);

                    if (masterIp.isPresent()) {
                        setFlowMasterIp(flow, masterIp.get(), jobLogger);
                        updateFlow(flow);

                    } else {
                        jobLogger.error("Timed out waiting " + spoolUpTimeoutInMinutes + " minutes for cluster to start. Shutting down cluster " + clusterId);
                        terminateCluster(getEmrClient(), clusterId);
                        updateFlow(flow);
                        return false;
                    }


                } catch (IOException | InvalidEmrConfigurationException e) {
                    jobLogger.error("Failed to resolve emr cluster configuration from properties: " + e);
                    jobLogger.error("Will run job without modifying configuration.");

                } catch (ExecutorManagerException e) {
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

        // Just for display purposes, hate printing null in logs
        if (clusterName == null) clusterName = "n/a";

        // We should have a cluster by now so lets get this puppy going
        if (clusterId != null) {
            setClusterProperty(flow, EMR_INTERNAL_CLUSTERID, clusterId);

            Cluster c = EmrUtils.findClusterById(getEmrClient(), clusterId);
            jobLogger.info("Cluster Summary - Id: " + c.getId());
            jobLogger.info("Cluster Summary - Status: " + c.getStatus());

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

            // Are the other flows sharing the cluster ok to shutdown the cluster?
            Boolean otherFlowsOkToShutdown = !clustersKeepAlive.contains(clusterId);

            // Log Debug
            jobLogger.info("Shutdown Process - Flow Status: " + flowStatus);
            jobLogger.info("Shutdown Process - Cluster Id: " + clusterId);
            jobLogger.info("Shutdown Process - Cluster Name: " + clusterName);
            jobLogger.info("Shutdown Process - Latch Count: " + count);
            jobLogger.info("Shutdown Process - Is ok with this flow to shutdown cluster: " + okForThisFlowToShutdownCluster);

            // If this is the last flow using the cluster
            if (count <= 0) {
                jobLogger.info("Shutdown Process - Is ok with the other flows sharing this cluster to shutdown the cluster: " + otherFlowsOkToShutdown);

                if (okForThisFlowToShutdownCluster && otherFlowsOkToShutdown) {
                    if (clusterId != null) {
                        jobLogger.info("Terminating cluster " + clusterId);
                        terminateCluster(getEmrClient(), clusterId);
                    } else {
                        jobLogger.warn("Cluster id was null, not terminating cluster!");
                    }

                } else if (!okForThisFlowToShutdownCluster) {
                    // If we are not shutting down the cluster now and it's not ok for the cluster to be shutdown from this specific flow's prespective, add note to that
                    clustersKeepAlive.add(clusterId);

                } else if (!otherFlowsOkToShutdown) {
                    // Not ok to shutdown with some other flow that used this cluster
                    jobLogger.warn("We are not shutting down cluster " + clusterId + " because it's not ok with one of the flows that used the same cluster");

                } else {
                    jobLogger.warn("Not terminating cluster based on execution properties.");
                }

            } else {
                jobLogger.info("Looks there are still other flows using cluster " + clusterId + " so we are not gonna shut it down");
            }

        } catch (Throwable error) {
            jobLogger.error("Error during shutdown process - Cluster Id: " + clusterId + ", Cluster Name: " + clusterName + ", Error: " + error.getMessage());
            error.printStackTrace();

        } finally {
            clustersKeepAlive.remove(clusterId);
            clusterFlows.remove(clusterName);
        }
    }

    public Optional<String> blockUntilReadyAndReturnMasterIp(String clusterId, int timeoutInMinutes, Logger
            jobLogger) {
        if (clusterId == null) return Optional.empty();

        jobLogger.info("Will wait for cluster to be ready for " + timeoutInMinutes + " minutes (" + EMR_CONF_SPOOLUP_TIMEOUT + ")");
        try {
            int tempTimeoutInMinutes = timeoutInMinutes;
            while (--tempTimeoutInMinutes > 0) {
                if (isClusterReady(getEmrClient(), clusterId)) {
                    Optional<String> masterIp = getMasterIP(getEmrClient(), clusterId);
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
        return new AmazonElasticMapReduceClient(getAWSCredentials()).withRegion(Regions.US_WEST_2);
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
}
