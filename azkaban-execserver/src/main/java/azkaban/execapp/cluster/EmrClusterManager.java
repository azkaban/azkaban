package azkaban.execapp.cluster;

import azkaban.execapp.AzkabanExecutorServer;
import azkaban.execapp.cluster.emr.InvalidEmrConfigurationException;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.Status;
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

import static azkaban.execapp.cluster.emr.EmrUtils.*;

/**
 * Created by jsoumet on 2/7/16 for azkaban.
 */
public class EmrClusterManager implements ClusterManager {

    private Logger logger = null;
    private static final String EMR_CONF_ENABLED = "cluster.emr.enabled";

    private static final String EMR_CONF_TERMINATE_ERROR = "cluster.emr.terminate.error";
    private static final String EMR_CONF_TERMINATE_COMPLETION = "cluster.emr.terminate.completion";

    private static final String EMR_CONF_SPOOLUP_TIMEOUT = "cluster.emr.spoolup.timeout";

    private static final String EMR_CONF_APP_CONFIGURATION_S3_PATH = "cluster.emr.app.configuration.s3.path";

    private static final String EMR_CONF_SELECT_ID = "cluster.emr.select.id";

    private static final String EMR_DEFAULT_APP_CONFIGURATION_S3_PATH = "s3://usw2-relateiq-emr-scripts/scripts/hadoo-emr-settings.json";

    private ExecutableFlow flow;
    private Props combinedProps = null;

    private String clusterName = null;
    private String clusterId = null;
    private String masterIp = null;
    private boolean runOnSpecificCluster = false;

    private RunJobFlowRequest emrClusterRequest = null;

    public EmrClusterManager(ExecutableFlow flow, Logger log) {
        this.flow = flow;
        this.logger = log != null ? log : Logger.getLogger(this.getClass());

        ExecutionOptions executionOptions = flow.getExecutionOptions();
        HashMap<String, Object> clusterProperties = executionOptions.getClusterProperties();
        combinedProps = new Props(propsFromMap(clusterProperties), AzkabanExecutorServer.getApp().getFlowRunnerManager().getGlobalProps());
        flow.getInputProps().getMapByPrefix("cluster.").forEach((key,val) -> combinedProps.put("cluster." + key, val));


        clusterName = "Azkaban - Transient Cluster - [" + flow.getFlowId() + ":" + flow.getExecutionId() + "]";
        logger.info("Loaded clusterManager for job " + flow.getFlowId() + ":" + flow.getExecutionId() + ".");
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
//TODO: change name to reflect environment
    @Override
    public boolean shouldCreateCluster() {

        printClusterProperties();

        // check if we're supposed to use a dedicated cluster - by default not
        if (!combinedProps.getBoolean(EMR_CONF_ENABLED, false)) {
            logger.info(EMR_CONF_ENABLED + " not found or set to false. Will not create a dedicated cluster for this flow.");
            return false;
        }


        // If requesting specific clusterId:
        if (combinedProps.get(EMR_CONF_SELECT_ID) != null) {
            clusterId = combinedProps.getString(EMR_CONF_SELECT_ID);
            logger.info(EMR_CONF_SELECT_ID + " was set to " + clusterId + ". Trying to use specific cluster to run this flow.");
            runOnSpecificCluster = isClusterActive(getEmrClient(), clusterId);
            logger.info("Cluster " + clusterId + " is " + (runOnSpecificCluster ? "ACTIVE" : "INACTIVE"));
            return runOnSpecificCluster;
        }

        // Create new cluster
        try {
            logger.info("Preparing new EMR cluster request to run this flow. Cluster name will be " + clusterName + ".");
            JobFlowInstancesConfig instancesConfig = createEMRClusterInstanceConfig(combinedProps);
            String jsonConfigUrl = combinedProps.getString(EMR_CONF_APP_CONFIGURATION_S3_PATH, EMR_DEFAULT_APP_CONFIGURATION_S3_PATH);
            Collection<Configuration> clusterConfigurations = getClusterJSONConfiguration(getS3Client(), jsonConfigUrl);
            emrClusterRequest = createRunJobFlowRequest(combinedProps, clusterName, instancesConfig, clusterConfigurations);
            logger.info("Cluster configuration for " + clusterName + " passed validation.");
            return true;
        } catch (IOException | InvalidEmrConfigurationException e) {
            logger.error("Failed to resolve emr cluster configuration from properties: " + e);
        }
        return false;
    }

    private void printClusterProperties() {
        {
            logger.info("Execution options:");
            combinedProps.getKeySet()
                    .stream()
                    .filter(k -> k.startsWith("cluster."))
                    .forEach(key -> logger.info(key + "=" + combinedProps.get(key)));
        }
    }

    @Override
    public boolean ensureClusterIsReady() {
        int timeoutInMinutes = combinedProps.getInt(EMR_CONF_SPOOLUP_TIMEOUT, 15);
        logger.info("Cluster spoolup timer set to " + timeoutInMinutes + " minutes (" + EMR_CONF_SPOOLUP_TIMEOUT + ")");
        try {
            // maybe spin up cluster
            if (clusterId == null) {
                logger.info("Initiating new cluster request on EMR.");
                RunJobFlowResult result = getEmrClient().runJobFlow(emrClusterRequest);
                clusterId = result.getJobFlowId();
                logger.info("New cluster created with id: " + clusterId);
            }
            int tempTimeoutInMinutes = timeoutInMinutes;
            while (--tempTimeoutInMinutes > 0) {
                if (isClusterReady(getEmrClient(), clusterId)) {
                    masterIp = getMasterIP(getEmrClient(), clusterId).orElseThrow(() -> new RuntimeException("Cluster " + clusterId + " is ready but failed to find master IP"));
                    logger.info("Cluster " + clusterId + " is ready! Spinup time was " + (timeoutInMinutes - tempTimeoutInMinutes) + " minutes.");
                    return true;
                }
                logger.info("Waiting for cluster " + clusterId + " to be ready... " + tempTimeoutInMinutes + " minutes remaining.");
                Thread.sleep(60000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        logger.error("Cluster failed to spool up after waiting for " + timeoutInMinutes + " mins.");
        return false;
    }

    @Override
    public void updateJobFlowProperties() {
        logger.info("Updating hadoop master ip with " + masterIp);
        flow.getInputProps().put("hadoop-inject.hadoop.master.ip", masterIp);
    }


    boolean shouldShutdown(Status flowStatus) {
        boolean terminateOnCompletion = combinedProps.getBoolean(EMR_CONF_TERMINATE_COMPLETION, true);
        boolean terminateOnError = combinedProps.getBoolean(EMR_CONF_TERMINATE_ERROR, true);

        switch (flowStatus) {
            case SUCCEEDED:
                logger.info("Flow succeeded. " + EMR_CONF_TERMINATE_COMPLETION + "=" + terminateOnCompletion);
                return terminateOnCompletion;
            case FAILED:
                logger.info("Flow failed. " + EMR_CONF_TERMINATE_ERROR + "=" + terminateOnError);
                return terminateOnError;
            case KILLED:
                logger.info("Flow was killed. " + EMR_CONF_TERMINATE_COMPLETION + "=" + terminateOnCompletion);
                return terminateOnCompletion;
            default:
                logger.info("Flow completed with unknown status: " + flowStatus + ". " + EMR_CONF_TERMINATE_COMPLETION + "=" + terminateOnCompletion);
                return terminateOnCompletion;
        }
    }

    @Override
    public void maybeTerminateCluster() {
        if (runOnSpecificCluster) {
            logger.info("Specific cluster was selected, will not terminate cluster: " + clusterId);
            return;
        }
        if (shouldShutdown(flow.getStatus())) {
            logger.info("Terminating cluster " + clusterId);
            terminateCluster(getEmrClient(), clusterId);
        } else {
            logger.info("Not terminating cluster based on properties.");
        }

    }

}
