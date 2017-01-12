/*
 * Copyright 2016 LinkedIn Corp.
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

import org.apache.log4j.PatternLayout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test output of PatternLayoutEscapedTest
 * It should be appending stack traces, escaping new lines, quotes, tabs and backslashes
 * This is necessary when we are logging these messages out as JSON objects
 */
public class PatternLayoutEscapedTest {
  private Logger logger = Logger.getLogger(this.getClass());
  private PatternLayout layout = new PatternLayoutEscaped();

  @Before
  public void beforeTest() {
    layout.setConversionPattern("%m");
  }

  @Test
  public void testWithException() {
    try {
      throw new Exception("This is an exception");
    } catch (Exception e) {
      LoggingEvent event = createEventWithException("There was an exception", e);
      // Stack trace might change if the codebase changes, but this prefix should always remain the same
      assertTrue(layout.format(event).startsWith("There was an exception\\njava.lang.Exception: This is an exception"));
    }
  }

  @Test
  public void testNewLine() {
    LoggingEvent event = createMessageEvent("This message contains \n new lines");
    assertTrue(layout.format(event).equals("This message contains \\n new lines"));
  }

  @Test
  public void testQuote() {
    LoggingEvent event = createMessageEvent("This message contains \" quotes");
    assertTrue(layout.format(event).equals("This message contains \\\" quotes"));
  }

  @Test
  public void testTab() {
    LoggingEvent event = createMessageEvent("This message contains a tab \t");
    assertTrue(layout.format(event).equals("This message contains a tab \\t"));
  }

  @Test
  public void testBackSlash() {
    LoggingEvent event = createMessageEvent("This message contains a backslash \\");
    assertTrue(layout.format(event).equals("This message contains a backslash \\\\"));
  }

  private LoggingEvent createMessageEvent(String message) {
    return createEventWithException(message, null);
  }

  private LoggingEvent createEventWithException(String message, Exception e) {
    return new LoggingEvent(this.getClass().getCanonicalName(),
        logger,
        0,
        Level.toLevel("INFO"),
        message,
        e);
  }
}
