package azkaban.execapp;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import azkaban.executor.ServerStatistics;
import azkaban.utils.JSONUtils;

public class StatisticsServletTest {
  private class MockStatisticsServlet extends ServerStatisticsServlet{
    /** */
    private static final long serialVersionUID = 1L;

    public  ServerStatistics getStastics(){
      return cachedstats;
    }

    public  long getUpdatedTime(){
      return lastRefreshedTime;
    }

    public void callPopulateStatistics(){
       this.populateStatistics(false);
    }

    public void callFillCpuUsage(ServerStatistics stats){
      this.fillCpuUsage(stats);}

    public void callFillRemainingMemoryPercent(ServerStatistics stats){
        this.fillRemainingMemoryPercent(stats);}
  }
  private MockStatisticsServlet statServlet = new MockStatisticsServlet();

  @Test
  public void testFillMemory()  {
    ServerStatistics stats = new ServerStatistics();
    this.statServlet.callFillRemainingMemoryPercent(stats);
    // assume any machine that runs this test should
    // have bash and top available and at least got some remaining memory.
    Assert.assertTrue(stats.getRemainingMemory() > 0);
    Assert.assertTrue(stats.getRemainingMemoryPercent() > 0);
  }

  @Test
  public void testFillCpu()  {
    ServerStatistics stats = new ServerStatistics();
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
    final long updatedTime = this.statServlet.getUpdatedTime();
    while (System.currentTimeMillis() - updatedTime < 1000){
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
    ServerStatistics stat = new ServerStatistics(0.1,1,2,new Date(),3,5);
    String jSonStr = JSONUtils.toJSON(stat);
    @SuppressWarnings("unchecked")
    Map<String,Object> jSonObj = (Map<String,Object>)JSONUtils.parseJSONFromString(jSonStr);
    ServerStatistics stat2 = ServerStatistics.fromJsonObject(jSonObj);
    Assert.assertTrue(stat.equals(stat2));
    }

  @Test
  public void testStatisticsJsonParserWNullDateValue() throws IOException  {
    ServerStatistics stat = new ServerStatistics(0.1,1,2,null,3,5);
    String jSonStr = JSONUtils.toJSON(stat);
    @SuppressWarnings("unchecked")
    Map<String,Object> jSonObj = (Map<String,Object>)JSONUtils.parseJSONFromString(jSonStr);
    ServerStatistics stat2 = ServerStatistics.fromJsonObject(jSonObj);
    Assert.assertTrue(stat.equals(stat2));
    }
}
