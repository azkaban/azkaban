package azkaban.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import azkaban.metric.inmemoryemitter.InMemoryHistoryNode;
import azkaban.metric.inmemoryemitter.InMemoryMetricEmitter;
import azkaban.utils.Props;
import java.util.Date;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/**
 * Azkaban Metric Manager Tests
 */
public class MetricManagerTest {

  MetricReportManager manager;
  FakeMetric metric;
  InMemoryMetricEmitter emitter;

  @Before
  public void setUp() throws Exception {
    this.manager = MetricReportManager.getInstance();
    this.metric = new FakeMetric(this.manager);
    this.manager.addMetric(this.metric);
    this.emitter = new InMemoryMetricEmitter(new Props());
    this.manager.addMetricEmitter(this.emitter);
  }

  /**
   * Test enable disable and status methods
   */
  @Test
  public void managerStatusTest() {
    assertNotNull("Singleton Failed to instantiate", this.manager);
    assertTrue("Failed to enable metric manager", MetricReportManager.isAvailable());
    this.manager.disableManager();
    assertFalse("Failed to disable metric manager", MetricReportManager.isAvailable());
    this.manager.enableManager();
    assertTrue("Failed to enable metric manager", MetricReportManager.isAvailable());
  }

  /**
   * Test adding and accessing metric methods
   */
  @Test
  public void managerMetricMaintenanceTest() {
    assertEquals("Failed to add metric", this.manager.getAllMetrics().size(), 1);
    assertTrue("Failed to add metric", this.manager.getAllMetrics().contains(this.metric));
    assertEquals("Failed to get metric by Name", this.manager.getMetricFromName("FakeMetric"),
        this.metric);
  }

  /**
   * Test adding, removing and accessing metric emitter.
   */
  @Test
  public void managerEmitterMaintenanceTest() {
    assertTrue("Failed to add Emitter", this.manager.getMetricEmitters().contains(this.emitter));

    final int originalSize = this.manager.getMetricEmitters().size();
    this.manager.removeMetricEmitter(this.emitter);
    assertEquals("Failed to remove emitter", this.manager.getMetricEmitters().size(),
        originalSize - 1);
    this.manager.addMetricEmitter(this.emitter);
  }

  /**
   * Test metric reporting methods, including InMemoryMetricEmitter methods
   */
  @Test
  public void managerEmitterHandlingTest() throws Exception {
    this.emitter.purgeAllData();
    final Date from = new Date();
    this.metric.notifyManager();

    Thread.sleep(2000);

    final List<InMemoryHistoryNode> nodes = this.emitter
        .getMetrics("FakeMetric", from, new Date(), false);

    assertEquals("Failed to report metric", 1, nodes.size());
    assertEquals("Failed to report metric", nodes.get(0).getValue(), 4);
  }
}
