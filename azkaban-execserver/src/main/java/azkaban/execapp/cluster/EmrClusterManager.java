package azkaban.execapp.cluster;

import azkaban.event.Event;
import azkaban.event.EventListener;
import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.FlowRunner;
import azkaban.execapp.cluster.emr.InvalidEmrConfigurationException;
import azkaban.executor.*;
import azkaban.utils.Props;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static azkaban.execapp.cluster.emr.EmrUtils.*;

/**
 * Created by jsoumet on 2/7/16 for azkaban.
 */
public class EmrClusterManager implements IClusterManager, EventListener {


    private enum ExecutionMode {
        DEFAULT_CLUSTER,
        CREATE_CLUSTER,
        SPECIFIC_CLUSTER
    }

    private Logger classLogger = Logger.getLogger(EmrClusterManager.class);


    private static final String EMR_CONF_ENABLED = "cluster.emr.enabled";
    private static final String EMR_CONF_TERMINATE_ERROR = "cluster.emr.terminate.error";
    private static final String EMR_CONF_TERMINATE_COMPLETION = "cluster.emr.terminate.completion";
    private static final String EMR_CONF_SELECT_ID = "cluster.emr.select.id";
    private static final String EMR_CONF_SPOOLUP_TIMEOUT = "cluster.emr.spoolup.timeout";

    private static final String EMR_CONF_APP_CONFIGURATION_S3_PATH = "cluster.emr.app.configuration.s3.path";
    private static final String EMR_DEFAULT_APP_CONFIGURATION_S3_PATH = "s3://usw2-relateiq-emr-scripts/scripts/hadoo-emr-settings.json";


    // internal flow props
    private static final String EMR_INTERNAL_CLUSTERID = "cluster.emr.internal.clusterid";
    private static final String EMR_INTERNAL_CLUSTERNAME = "cluster.emr.internal.clustername";
    private static final String EMR_INTERNAL_EXECUTION_MODE = "cluster.emr.execution.mode";

    private Props globalProps = null;
    private ExecutorLoader executorLoader = null;

    public EmrClusterManager(Props serverProps) {
        classLogger.info("Initialized " + this.getClass().getName());
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

    @Override
    public void handleEvent(Event event) {
        if (event.getRunner() instanceof FlowRunner) {
            FlowRunner runner = (FlowRunner) event.getRunner();
            ExecutableFlow flow = runner.getExecutableFlow();
            Logger logger = Logger.getLogger(flow.getExecutionId() + "." + flow.getFlowId());
            logger.info(this.getClass().getName() + " handling " + event.getType() + " " + event.getTime());

            switch (event.getType()) {
                case FLOW_STARTED:
                    boolean runStatus = createClusterAndConfigureJob(flow, logger);
                    if (!runStatus) runner.kill(this.getClass().getName());
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

        int spoolUpTimeoutInMinutes = combinedProps.getInt(EMR_CONF_SPOOLUP_TIMEOUT, 15);
        String clusterId = null;

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
                    String clusterName = "Azkaban - Transient Cluster - [" + flow.getFlowId() + ":" + flow.getExecutionId() + "]";
                    setClusterProperty(flow, EMR_INTERNAL_CLUSTERNAME, clusterName);
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
                    Optional<String> masterIp = blockUntilReadyAndReturnMasterIp(clusterId, spoolUpTimeoutInMinutes, jobLogger);
                    if (masterIp.isPresent()) {
                        setFlowMasterIp(flow, masterIp.get(), jobLogger);
                    } else {
                        jobLogger.error("Timed out waiting " + spoolUpTimeoutInMinutes + " minutes for cluster to start. Shutting down cluster " + clusterId);
                        terminateCluster(getEmrClient(), clusterId);
                        return false;
                    }
                } catch (IOException | InvalidEmrConfigurationException e) {
                    jobLogger.error("Failed to resolve emr cluster configuration from properties: " + e);
                    jobLogger.error("Will run job without modifying configuration.");
                } catch (ExecutorManagerException e) {
                    e.printStackTrace();
                }

                break;
            case DEFAULT_CLUSTER:
            default:
                // TODO: find default cluster from conf
                // right now.. do nothing

                break;
        }
        if (clusterId != null) setClusterProperty(flow, EMR_INTERNAL_CLUSTERID, clusterId);
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
                if (shouldShutdown(combinedProps, flow.getStatus(), jobLogger)) {
                    String clusterId = combinedProps.get(EMR_INTERNAL_CLUSTERID);
                    if (clusterId != null) {
                        jobLogger.info("Terminating cluster " + clusterId);
                        terminateCluster(getEmrClient(), clusterId);
                    } else {
                        jobLogger.info("ClusterId was null. Not terminating cluster.");
                    }
                } else {
                    jobLogger.info("Not terminating cluster based on execution properties.");
                }
                break;
            default:
                jobLogger.info("Execution mode was " + executionMode + ", so cluster will not be terminated.");
                break;
        }
    }




    public Optional<String> blockUntilReadyAndReturnMasterIp(String clusterId, int timeoutInMinutes, Logger jobLogger) {
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
        flow.getInputProps().getMapByPrefix("cluster.").forEach((key,val) -> combinedProps.put("cluster." + key, val));
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
}
