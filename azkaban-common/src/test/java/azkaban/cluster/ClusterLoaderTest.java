package azkaban.cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link ClusterLoader}.
 */
public class ClusterLoaderTest {
  @Rule
  public TemporaryFolder testDir = new TemporaryFolder();

  /**
   * Sanity check when a single cluster is configured.
   */
  @Test
  public void testLoadingSingleCluster() throws IOException {
    final File clusterDir = this.testDir.newFolder("single-cluster");
    final File clusterConfig = new File(clusterDir, ClusterLoader.CLUSTER_CONF_FILE);
    try (final Writer fileWriter = new OutputStreamWriter(
        new FileOutputStream(clusterConfig), StandardCharsets.UTF_8)) {
      fileWriter
          .write("hadoop.security.manager.class=azkaban.security.HadoopSecurityManager_H_2_0\n");
      fileWriter.write("A=a\n");
      fileWriter.write("B=b\n");
    }
    final ClusterRegistry clusterRegistry = new ClusterRegistry();

    ClusterLoader.loadCluster(clusterDir, clusterRegistry);

    final Cluster cluster = clusterRegistry.getCluster(clusterDir.getName());
    Assert.assertEquals(clusterDir.getName(), cluster.clusterId);
  }

  /**
   * Verify an exception is thrown properly when a single cluster is configured
   * but with its cluster.properties file missing.
   */
  @Test (expected = FileNotFoundException.class)
  public void testLoadingSingleClusterWithMissingClusterConfig() throws IOException {
    final File clusterDir = this.testDir.newFolder("single-cluster-no-config");
    final ClusterRegistry clusterRegistry = new ClusterRegistry();

    ClusterLoader.loadCluster(clusterDir, clusterRegistry);
  }

  /**
   * Sanity check when multiple clusters are configured.
   */
  @Test
  public void testLoadingMultipleClusters() throws IOException {
    final File clustersDir = new File(
        getClass().getClassLoader().getResource("clusters").getFile());
    final ClusterRegistry clusterRegistry = new ClusterRegistry();

    final ClusterLoader clusterLoader = new ClusterLoader(clustersDir, clusterRegistry);

    final Cluster defaultCluster = clusterRegistry.getCluster("default");
    Assert.assertEquals("default", defaultCluster.clusterId);
    final Cluster another = clusterRegistry.getCluster("another");
    Assert.assertEquals("another", another.clusterId);
  }
}
