package azkaban.execapp.cluster;

/**
 * Created by jsoumet on 2/7/16 for azkaban.
 */
public interface ClusterManager {
    boolean shouldCreateCluster();

    boolean ensureClusterIsReady();

    void updateJobFlowProperties();

    void maybeTerminateCluster();
}