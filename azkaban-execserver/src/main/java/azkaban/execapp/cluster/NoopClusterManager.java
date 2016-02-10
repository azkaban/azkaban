package azkaban.execapp.cluster;

/**
 * Created by jsoumet on 2/7/16 for azkaban.
 */
public class NoopClusterManager implements ClusterManager {

    @Override
    public boolean shouldCreateCluster() {
        return false;
    }

    @Override
    public boolean createClusterBlocking(int timeoutInMinutes) {
        return true;
    }

    @Override
    public void updateJobFlow() {

    }

}
