package azkaban.execapp.cluster;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

import java.util.HashMap;

/**
 * Created by jsoumet on 2/7/16 for azkaban.
 */
public class EmrClusterManager implements ClusterManager {

    private static final String CLUSTER_USE_TRANSIENT_CLUSTER = "cluster.use.transient.cluster";

    private ExecutableFlow flow;
    private ExecutionOptions executionOptions;
    private HashMap<String, Object> clusterProperties;
    private String clusterName;

    public EmrClusterManager(ExecutableFlow flow) {
        this.flow = flow;
        executionOptions = flow.getExecutionOptions();
        clusterProperties = executionOptions.getClusterProperties();
        clusterName = "Azkaban - Transient Cluster - [" + flow.getFlowId() + ":" + flow.getExecutionId() + "]";
    }

    private AWSCredentials getCredentials() {
        DefaultAWSCredentialsProviderChain providerChain = new DefaultAWSCredentialsProviderChain();
        return providerChain.getCredentials();
    }

    @Override
    public boolean shouldCreateCluster() {
        return clusterProperties.containsKey(CLUSTER_USE_TRANSIENT_CLUSTER) && ((boolean) clusterProperties.getOrDefault(CLUSTER_USE_TRANSIENT_CLUSTER, false));
    }

    @Override
    public boolean createClusterBlocking(int timeoutInMinutes) {
        try {
            Thread.sleep(timeoutInMinutes * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void updateJobFlow() {
        flow.getInputProps().put("hadoop-inject.hadoop.master.ip", "10.31.48.87");

    }
}
