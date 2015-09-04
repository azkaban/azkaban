package azkaban.execapp;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import azkaban.executor.Statistics;
import azkaban.utils.JSONUtils;

public class StatisticsServletTest {
  private class MockStatisticsServlet extends StatisticsServlet{
    /** */
    private static final long serialVersionUID = 1L;

    public  Statistics getStastics(){
      return cachedstats;
    }

    public  Date getUpdatedTime(){
      return lastRefreshedTime;
    }

    public void callPopulateStatistics(){
       this.populateStatistics(false);
    }

    public void callFillCpuUsage(Statistics stats){
      this.fillCpuUsage(stats);}

    public void callFillRemainingMemoryPercent(Statistics stats){
        this.fillRemainingMemoryPercent(stats);}
  }
  private MockStatisticsServlet statServlet = new MockStatisticsServlet();

  @Test
  public void testFillMemory()  {
    Statistics stats = new Statistics();
    this.statServlet.callFillRemainingMemoryPercent(stats);
    // assume any machine that runs this test should
    // have bash and top available and at least got some remaining memory.
    Assert.assertTrue(stats.getRemainingMemory() > 0);
    Assert.assertTrue(stats.getRemainingMemoryPercent() > 0);
  }

  @Test
  public void testFillCpu()  {
    Statistics stats = new Statistics();
    this.statServlet.callFillCpuUsage(stats);
    Assert.assertTrue(stats.getCpuUsage() > 0);
  }

  @Test
  public void testPopulateStatistics()  {
    this.statServlet.callPopulateStatistics();
    Assert.assertNotNull(this.statServlet.getStastics());
    Assert.assertTrue(this.statServlet.getStastics().getRemainingMemory() > 0);
    Assert.assertTrue(this.statServlet.getStastics().getRemainingMemoryPercent() > 0);
    Assert.assertTrue(this.statServlet.getStastics().getCpuUsage() > 0);
  }

  @Test
  public void testPopulateStatisticsCache()  {
    this.statServlet.callPopulateStatistics();
    final Date updatedTime = this.statServlet.getUpdatedTime();
    while (new Date().getTime() - updatedTime.getTime() < 1000){
      this.statServlet.callPopulateStatistics();
      Assert.assertEquals(updatedTime, this.statServlet.getUpdatedTime());
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {}

    // make sure cache expires after timeout.
    this.statServlet.callPopulateStatistics();
    Assert.assertNotEquals(updatedTime, this.statServlet.getUpdatedTime());
  }

  @Test
  public void testStatisticsJsonParser() throws IOException  {
    Statistics stat = new Statistics(0.1,1,2,new Date(),3,4,5,5);
    String jSonStr = JSONUtils.toJSON(stat);
    @SuppressWarnings("unchecked")
    Map<String,Object> jSonObj = (Map<String,Object>)JSONUtils.parseJSONFromString(jSonStr);
    Statistics stat2 = Statistics.fromJsonObject(jSonObj);
    Assert.assertTrue(stat.equals(stat2));
    }

  @Test
  public void testStatisticsJsonParserWNullDateValue() throws IOException  {
    Statistics stat = new Statistics(0.1,1,2,null,3,4,5,5);
    String jSonStr = JSONUtils.toJSON(stat);
    @SuppressWarnings("unchecked")
    Map<String,Object> jSonObj = (Map<String,Object>)JSONUtils.parseJSONFromString(jSonStr);
    Statistics stat2 = Statistics.fromJsonObject(jSonObj);
    Assert.assertTrue(stat.equals(stat2));
    }
}
