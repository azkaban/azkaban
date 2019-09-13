/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

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

  private static final String[] browserVariants = {chromeOnMac, fireFoxOnMac,
      safariOnMac, chromeOnLinux, fireFoxOnLinux};

  private static final String[] BROWSER_NAMES = {"AppleWebKit", "Gecko",
      "Chrome"};

  @Test
  public void isBrowser() throws Exception {

    for (final String browser : browserVariants) {
      Assert.assertTrue(browser, StringUtils.isFromBrowser(browser));
    }
  }

  @Test
  public void notBrowserWithLowercase() throws Exception {

    for (final String browser : browserVariants) {
      Assert.assertFalse(browser.toLowerCase(),
          StringUtils.isFromBrowser(browser.toLowerCase()));
    }
  }

  @Test
  public void notBrowser() throws Exception {
    final String testStr = "curl";
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
    for (final String name : BROWSER_NAMES) {
      Assert.assertTrue(StringUtils.isFromBrowser(name + " is awesome"));
    }
  }

  @Test
  public void endsWithBrowserName() {
    for (final String name : BROWSER_NAMES) {
      Assert.assertTrue(StringUtils.isFromBrowser("awesome is" + name));
    }
  }

  @Test
  public void containsBrowserName() {
    for (final String name : BROWSER_NAMES) {
      Assert.assertTrue(StringUtils.isFromBrowser("awesome " + name + " is"));
    }
  }

  @Test
  public void testShellQuote() {
    Assert.assertEquals("!foo!", StringUtils.shellQuote("foo", '!'));
    Assert.assertEquals("\u0000!\u0000",
        StringUtils.shellQuote("!", '\u0000'));
  }

  @Test
  public void testJoin() {
    final Collection<String> list = new ArrayList<>();
    list.add("!");
    list.add("?");

    Assert.assertEquals("!", StringUtils.join(
        new ArrayList<>(Arrays.asList("")), "!"));
    Assert.assertEquals("", StringUtils.join(
        new ArrayList<>(), null));
    Assert.assertEquals("foo$bar$", StringUtils.join(
        new ArrayList<>(Arrays.asList("foo", "bar")), "$"));
    Assert.assertEquals("!foo?foo", StringUtils.join(list, "foo"));
  }

  @Test
  public void testJoin2() {
    Assert.assertEquals("0000", StringUtils.join2(
        new ArrayList<>(Arrays.asList("0000")), null));
    Assert.assertEquals("", StringUtils.join2(
        new ArrayList<>(), null));
    Assert.assertEquals("!!$$$??", StringUtils.join2(
        new ArrayList<>(Arrays.asList("!!", "??")), "$$$"));
  }

  @Test
  public void testIsFromBrowser() {
    Assert.assertFalse(StringUtils.isFromBrowser(null));
    Assert.assertFalse(StringUtils.isFromBrowser("foo"));

    Assert.assertTrue(StringUtils.isFromBrowser("*Gecko"));
    Assert.assertTrue(StringUtils.isFromBrowser("*Chrome"));
    Assert.assertTrue(StringUtils.isFromBrowser("*Trident"));
    Assert.assertTrue(StringUtils.isFromBrowser("*AppleWebKit"));
  }
}
