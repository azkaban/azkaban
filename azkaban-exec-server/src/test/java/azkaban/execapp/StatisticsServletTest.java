package azkaban.execapp;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import azkaban.executor.ExecutorInfo;

@Ignore
public class StatisticsServletTest {
  private class MockStatisticsServlet extends ServerStatisticsServlet {
    /** */
    private static final long serialVersionUID = 1L;

    public ExecutorInfo getStastics() {
      return cachedstats;
    }

    public long getUpdatedTime() {
      return lastRefreshedTime;
    }

    public void callPopulateStatistics() {
      this.populateStatistics(false);
    }

    public void callFillCpuUsage(ExecutorInfo stats) {
      this.fillCpuUsage(stats);
    }

    public void callFillRemainingMemoryPercent(ExecutorInfo stats) {
      this.fillRemainingMemoryPercent(stats);
    }
  }
  private MockStatisticsServlet statServlet = new MockStatisticsServlet();

  @Test
  public void testFillMemory() {
    ExecutorInfo stats = new ExecutorInfo();
    this.statServlet.callFillRemainingMemoryPercent(stats);
    // assume any machine that runs this test should
    // have bash and top available and at least got some remaining memory.
    Assert.assertTrue(stats.getRemainingMemoryInMB() > 0);
    Assert.assertTrue(stats.getRemainingMemoryPercent() > 0);
  }

  @Test
  public void testFillCpu() {
    ExecutorInfo stats = new ExecutorInfo();
    this.statServlet.callFillCpuUsage(stats);
    Assert.assertTrue(stats.getCpuUsage() > 0);
  }

  @Test
  public void testPopulateStatistics() {
    this.statServlet.callPopulateStatistics();
    Assert.assertNotNull(this.statServlet.getStastics());
    Assert.assertTrue(this.statServlet.getStastics().getRemainingMemoryInMB() > 0);
    Assert.assertTrue(this.statServlet.getStastics().getRemainingMemoryPercent() > 0);
    Assert.assertTrue(this.statServlet.getStastics().getCpuUsage() > 0);
  }

  @Test
  public void testPopulateStatisticsCache() {
    this.statServlet.callPopulateStatistics();
    final long updatedTime = this.statServlet.getUpdatedTime();
    while (System.currentTimeMillis() - updatedTime < 1000) {
      this.statServlet.callPopulateStatistics();
      Assert.assertEquals(updatedTime, this.statServlet.getUpdatedTime());
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }

    // make sure cache expires after timeout.
    this.statServlet.callPopulateStatistics();
    Assert.assertNotEquals(updatedTime, this.statServlet.getUpdatedTime());
  }
}
