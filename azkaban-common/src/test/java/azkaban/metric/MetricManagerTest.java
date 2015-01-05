package azkaban.metric;

import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import azkaban.metric.inmemoryemitter.InMemoryHistoryNode;
import azkaban.metric.inmemoryemitter.InMemoryMetricEmitter;
import azkaban.utils.Props;
import static org.junit.Assert.*;

/**
 * Azkaban Metric Manager Tests
 */
public class MetricManagerTest {
  MetricReportManager manager;
  FakeMetric metric;
  InMemoryMetricEmitter emitter;

  @Before
  public void setUp() throws Exception {
    manager = MetricReportManager.getInstance();
    metric = new FakeMetric(manager);
    manager.addMetric(metric);
    emitter = new InMemoryMetricEmitter(new Props());
    manager.addMetricEmitter(emitter);
  }

  /**
   * Test enable disable and status methods
   */
  @Test
  public void managerStatusTest() {
    assertNotNull("Singleton Failed to instantiate", manager);
    assertTrue("Failed to enable metric manager", MetricReportManager.isAvailable());
    manager.disableManager();
    assertFalse("Failed to disable metric manager", MetricReportManager.isAvailable());
    manager.enableManager();
    assertTrue("Failed to enable metric manager", MetricReportManager.isAvailable());
  }

  /**
   * Test adding and accessing metric methods
   */
  @Test
  public void managerMetricMaintenanceTest() {
    assertEquals("Failed to add metric", manager.getAllMetrics().size(), 1);
    assertTrue("Failed to add metric", manager.getAllMetrics().contains(metric));
    assertEquals("Failed to get metric by Name", manager.getMetricFromName("FakeMetric"), metric);
  }

  /**
   * Test adding, removing and accessing metric emitter.
   */
  @Test
  public void managerEmitterMaintenanceTest() {
    assertTrue("Failed to add Emitter", manager.getMetricEmitters().contains(emitter));

    int originalSize = manager.getMetricEmitters().size();
    manager.removeMetricEmitter(emitter);
    assertEquals("Failed to remove emitter", manager.getMetricEmitters().size(), originalSize - 1);
    manager.addMetricEmitter(emitter);
  }

  /**
   * Test metric reporting methods, including InMemoryMetricEmitter methods
   * @throws Exception
   */
  @Test
  public void managerEmitterHandlingTest() throws Exception {
    emitter.purgeAllData();
    Date from = new Date();
    metric.notifyManager();

    synchronized (this) {
      try {
        wait(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    Date to = new Date();
    List<InMemoryHistoryNode> nodes = emitter.getMetrics("FakeMetric", from, to, false);

    assertEquals("Failed to report metric", 1, nodes.size());
    assertEquals("Failed to report metric", nodes.get(0).getValue(), 4);
  }
}
