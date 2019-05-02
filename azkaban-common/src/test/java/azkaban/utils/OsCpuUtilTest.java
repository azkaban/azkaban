package azkaban.utils;


import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import azkaban.utils.OsCpuUtil.CpuStats;
import org.junit.Assert;
import org.junit.Test;

public class OsCpuUtilTest {

  private final OsCpuUtil osCpuUtil = new OsCpuUtil(1);

  @Test
  public void testGetCpuStatsFromLine() {
    final String line = "cpu  1     2 4 8    16    32 64 128 256          512 ";
    final CpuStats cpuStats = this.osCpuUtil.getCpuStatsFromLine(line);
    assertThat(cpuStats.getSysUptime()).isEqualTo(255);
    assertThat(cpuStats.getTimeCpuIdle()).isEqualTo(24);
  }

  @Test
  public void testGetCpuStatsFromLineWithInvalidInput() {
    // line with fewer values than expected
    String line = "cpu  7     4 3 6    2";
    Assert.assertNull(this.osCpuUtil.getCpuStatsFromLine(line));

    // line with decimal values
    line = "cpu  7.1     4.2 3 6    2    2 0 0 0";
    Assert.assertNull(this.osCpuUtil.getCpuStatsFromLine(line));
  }
}
