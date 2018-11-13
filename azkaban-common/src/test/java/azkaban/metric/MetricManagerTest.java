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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

/**
 * Azkaban Metric Manager Tests
 */
public class MetricManagerTest {

  MetricReportManager manager;
  FakeMetric metric;
  InMemoryMetricEmitter emitter;
  MetricEmitterWrapper emitterWrapper;

  @Before
  public void setUp() throws Exception {
    this.manager = MetricReportManager.getInstance();
    this.metric = new FakeMetric(this.manager);
    this.manager.addMetric(this.metric);
    this.emitter = new InMemoryMetricEmitter(new Props());
    this.emitterWrapper = new MetricEmitterWrapper();
    this.manager.addMetricEmitter(this.emitterWrapper);
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
    assertTrue("Failed to add Emitter",
        this.manager.getMetricEmitters().contains(this.emitterWrapper));

    final int originalSize = this.manager.getMetricEmitters().size();
    this.manager.removeMetricEmitter(this.emitterWrapper);
    assertEquals("Failed to remove emitter", this.manager.getMetricEmitters().size(),
        originalSize - 1);
    this.manager.addMetricEmitter(this.emitterWrapper);
  }

  /**
   * Test metric reporting methods, including InMemoryMetricEmitter methods
   */
  @Test
  public void managerEmitterHandlingTest() throws Exception {

    // metrics use System.currentTimeMillis, so that method should be the millis provider
    final DateTime aboutNow = new DateTime(System.currentTimeMillis());

    this.emitter.purgeAllData();

    final Date from = aboutNow.minusMinutes(1).toDate();
    this.metric.notifyManager();

    this.emitterWrapper.countDownLatch.await(10L, TimeUnit.SECONDS);

    final Date to = aboutNow.plusMinutes(1).toDate();
    final List<InMemoryHistoryNode> nodes = this.emitter.getMetrics("FakeMetric", from, to, false);

    assertEquals("Failed to report metric", 1, nodes.size());
    assertEquals("Failed to report metric", nodes.get(0).getValue(), 4);
  }

  private class MetricEmitterWrapper implements IMetricEmitter {

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void reportMetric(final IMetric<?> metric) throws MetricException {
      MetricManagerTest.this.emitter.reportMetric(metric);
      this.countDownLatch.countDown();
    }

    @Override
    public void purgeAllData() throws MetricException {
      MetricManagerTest.this.emitter.purgeAllData();
    }
  }
}
