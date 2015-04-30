package azkaban.utils;

import org.junit.Assert;
import org.junit.Test;

public class MemUtilsTest {
  @Test
  public void testConversion() {
    Assert.assertEquals(Utils.parseMemString("1024"), 1L);
    Assert.assertEquals(Utils.parseMemString("1K"), 1L);
    Assert.assertEquals(Utils.parseMemString("1M"), 1024L);
    Assert.assertEquals(Utils.parseMemString("1G"), 1024L * 1024L);

    Assert.assertEquals(Utils.parseMemString("1k"), 1L);
    Assert.assertEquals(Utils.parseMemString("1m"), 1024L);
    Assert.assertEquals(Utils.parseMemString("1g"), 1024L * 1024L);

    Assert.assertEquals(Utils.parseMemString("5000"), 4L);
    Assert.assertEquals(Utils.parseMemString("1024K"), 1024L);
    Assert.assertEquals(Utils.parseMemString("512M"), 512 * 1024L);
    Assert.assertEquals(Utils.parseMemString("2G"), 2L * 1024L * 1024L);
  }

  @Test
  public void testBadFormat() {
    badFormatHelper("1KB");
    badFormatHelper("1MB");
    badFormatHelper("1GB");
    
    badFormatHelper("1kb");
    badFormatHelper("1mb");
    badFormatHelper("1gb");

    badFormatHelper("100.5K");
    badFormatHelper("512.8M");
    badFormatHelper("0.5G");

    badFormatHelper("100b");
    badFormatHelper("100f");
    badFormatHelper("100abcdc");
  }
  
  private void badFormatHelper(String str) {
    try {
      Utils.parseMemString(str);
      Assert.fail("should get a runtime exception");
    } catch (Exception e) {
      Assert.assertEquals(e instanceof NumberFormatException, true);
    }
  }
}
