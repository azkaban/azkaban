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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TimeUtilsTest {

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFormatDateTime() {
    Assert.assertEquals("-",
        TimeUtils.formatDateTime(-8_832L));
    Assert.assertEquals("1970-01-01 00:00:08",
        TimeUtils.formatDateTime(8_832L));
  }

  @Test
  public void testFormatDateTimeZone() {
    Assert.assertEquals("1970/01/01 00:00:08 UTC",
        TimeUtils.formatDateTimeZone(8_832L));
  }

  @Test
  public void testFormatDuration() {
    Assert.assertEquals("-",
        TimeUtils.formatDuration(-1L, -513L));
    Assert.assertEquals("7 sec",
        TimeUtils.formatDuration(5098175271827214151L,
            5098175271827221543L));
    Assert.assertEquals("1m 1s",
        TimeUtils.formatDuration(-65793L, -4609L));
    Assert.assertEquals("1h 14m 1s",
        TimeUtils.formatDuration(-4462057L, -20994L));
    Assert.assertEquals("1290d 15h 0m",
        TimeUtils.formatDuration(-833737520625L, -722227520546L));
  }

  @Test
  public void testFormatPeriod() {
    Assert.assertEquals("null",
        TimeUtils.formatPeriod(null));
    Assert.assertEquals("5 year(s)",
        TimeUtils.formatPeriod(TimeUtils.parsePeriodString("5y")));
    Assert.assertEquals("12 month(s)",
        TimeUtils.formatPeriod(TimeUtils.parsePeriodString("12M")));
    Assert.assertEquals("10 week(s)",
        TimeUtils.formatPeriod(TimeUtils.parsePeriodString("10w")));
    Assert.assertEquals("2 day(s)",
        TimeUtils.formatPeriod(TimeUtils.parsePeriodString("2d")));
    Assert.assertEquals("24 hour(s)",
        TimeUtils.formatPeriod(TimeUtils.parsePeriodString("24h")));
    Assert.assertEquals("60 minute(s)",
        TimeUtils.formatPeriod(TimeUtils.parsePeriodString("60m")));
    Assert.assertEquals("120 second(s)",
        TimeUtils.formatPeriod(TimeUtils.parsePeriodString("120s")));
  }

  @Test
  public void testParsePeriodString() throws NullPointerException {
    Assert.assertNull(
        TimeUtils.parsePeriodString("7n"));

    Assert.assertEquals("Years",
        TimeUtils.parsePeriodString("5y").getPeriodType().getName());
    Assert.assertEquals("Months",
        TimeUtils.parsePeriodString("12M").getPeriodType().getName());
    Assert.assertEquals("Weeks",
        TimeUtils.parsePeriodString("10w").getPeriodType().getName());
    Assert.assertEquals("Days",
        TimeUtils.parsePeriodString("2d").getPeriodType().getName());
    Assert.assertEquals("Hours",
        TimeUtils.parsePeriodString("24h").getPeriodType().getName());
    Assert.assertEquals("Minutes",
        TimeUtils.parsePeriodString("60m").getPeriodType().getName());
    Assert.assertEquals("Seconds",
        TimeUtils.parsePeriodString("120s").getPeriodType().getName());
  }

  @Test
  public void testParsePeriodStringException() throws IllegalArgumentException {
    thrown.expect(IllegalArgumentException.class);
    TimeUtils.parsePeriodString("120S");
  }

  @Test
  public void testCreatePeriodString() {
    Assert.assertEquals("null", TimeUtils.
        createPeriodString(null));

    Assert.assertEquals("5y", TimeUtils.
        createPeriodString(TimeUtils.parsePeriodString("5y")));
    Assert.assertEquals("12M", TimeUtils.
        createPeriodString(TimeUtils.parsePeriodString("12M")));
    Assert.assertEquals("10w", TimeUtils.
        createPeriodString(TimeUtils.parsePeriodString("10w")));
    Assert.assertEquals("2d", TimeUtils.
        createPeriodString(TimeUtils.parsePeriodString("2d")));
    Assert.assertEquals("24h", TimeUtils.
        createPeriodString(TimeUtils.parsePeriodString("24h")));
    Assert.assertEquals("60m", TimeUtils.
        createPeriodString(TimeUtils.parsePeriodString("60m")));
    Assert.assertEquals("120s", TimeUtils.
        createPeriodString(TimeUtils.parsePeriodString("120s")));
  }
}
