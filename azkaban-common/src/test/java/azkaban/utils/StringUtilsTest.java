package azkaban.utils;

import org.junit.Assert;
import org.junit.Test;

public class StringUtilsTest {

  private static final String chromeOnMac =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.155 Safari/537.36";
  private static final String fireFoxOnMac =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:40.0) Gecko/20100101 Firefox/40.0";
  private static final String safariOnMac =
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2";
  private static final String chromeOnLinux =
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36";
  private static final String fireFoxOnLinux =
      "Mozilla/5.0 (X11; Linux x86_64; rv:39.0) Gecko/20100101 Firefox/39.0";

  private static final String[] browserVariants = { chromeOnMac, fireFoxOnMac,
      safariOnMac, chromeOnLinux, fireFoxOnLinux };

  private static final String[] BROWSER_NAMES = { "AppleWebKit", "Gecko",
      "Chrome" };

  @Test
  public void isBrowser() throws Exception {

    for (String browser : browserVariants) {
      Assert.assertTrue(browser, StringUtils.isFromBrowser(browser));
    }
  }

  @Test
  public void notBrowserWithLowercase() throws Exception {

    for (String browser : browserVariants) {
      Assert.assertFalse(browser.toLowerCase(),
          StringUtils.isFromBrowser(browser.toLowerCase()));
    }
  }

  @Test
  public void notBrowser() throws Exception {
    String testStr = "curl";
    Assert.assertFalse(testStr, StringUtils.isFromBrowser(testStr));
  }

  @Test
  public void emptyBrowserString() throws Exception {

    Assert.assertFalse("empty string", StringUtils.isFromBrowser(""));
  }

  @Test
  public void nullBrowserString() throws Exception {

    Assert.assertFalse("null string", StringUtils.isFromBrowser(null));
  }

  @Test
  public void startsWithBrowserName() {
    for (String name : BROWSER_NAMES) {
      Assert.assertTrue(StringUtils.isFromBrowser(name + " is awesome"));
    }
  }

  @Test
  public void endsWithBrowserName() {
    for (String name : BROWSER_NAMES) {
      Assert.assertTrue(StringUtils.isFromBrowser("awesome is" + name));
    }
  }

  @Test
  public void containsBrowserName() {
    for (String name : BROWSER_NAMES) {
      Assert.assertTrue(StringUtils.isFromBrowser("awesome " + name + " is"));
    }
  }
}
