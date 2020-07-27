package azkaban.cluster;

import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * An implementation of {@link ClusterRouter} that routes jobs to the UNKNOWN cluster
 * so that the cluster implicitly loaded through Azkaban JVM will be used.
 */
public class DisabledClusterRouter extends ClusterRouter {
  public DisabledClusterRouter(final ClusterRegistry clusterRegistry, final Configuration configuration) {
    super(clusterRegistry, configuration);
  }

  @VisibleForTesting
  public DisabledClusterRouter() {
    super(new ClusterRegistry(), new Configuration());
  }

  @Override
  public Cluster getCluster(final String jobId, final Props jobProps,
      final Logger jobLogger, final Collection<String> componentDependency) {
    return Cluster.UNKNOWN;
  }

  @Override
  public Cluster getCluster(final String clusterId) {
    return Cluster.UNKNOWN;
  }
}
