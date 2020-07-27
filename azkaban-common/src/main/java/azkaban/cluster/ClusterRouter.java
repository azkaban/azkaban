package azkaban.cluster;

import azkaban.utils.Props;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import org.apache.log4j.Logger;

public abstract class ClusterRouter {
  protected final ClusterRegistry clusterRegistry;
  protected final Configuration configuration;

  public ClusterRouter(final ClusterRegistry clusterRegistry, final Configuration configuration) {
    this.clusterRegistry = clusterRegistry;
    this.configuration = configuration;
  }
  /**
   * Gets the information of the cluster that a job should be submitted to.
   */
  public abstract Cluster getCluster(final String jobId, final Props jobProps,
      final Logger jobLogger,
      final Collection<String> componentDependency);

  /**
   * Get the information of a cluster given its id.
   */
  public abstract Cluster getCluster(final String clusterId);
}
