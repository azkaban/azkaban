package azkaban.db;

import static org.junit.Assert.assertEquals;

import azkaban.metrics.MetricsManager;
import azkaban.metrics.MetricsTestUtility;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;

public class DBMetricsTest {
  private MetricsTestUtility testUtil;
  private DBMetrics metrics;

  @Before
  public void setUp() {
    final MetricRegistry metricRegistry = new MetricRegistry();
    this.testUtil = new MetricsTestUtility(metricRegistry);
    this.metrics = new DBMetrics(new MetricsManager(metricRegistry));
  }

  @Test
  public void testDBConnectionTimeMetrics() {
    this.metrics.setDBConnectionTime(14);
    assertEquals(14, this.testUtil.getGaugeValue("dbConnectionTime"));
  }
}
