/*
 * Copyright 2019 LinkedIn Corp.
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.DateTimeException;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;


public class TimeUtilsTest {

  @Test
  public void testTimeEscapedOver() throws InterruptedException {
    final long baseTime = System.currentTimeMillis();
    TimeUnit.SECONDS.sleep(1);
    Assert.assertTrue(TimeUtils.timeEscapedOver(baseTime, 1));
    Assert.assertFalse(TimeUtils.timeEscapedOver(baseTime, 2));
  }

  @Test
  public void testDayEscapedOver() throws InterruptedException {
    final long baseTime = System.currentTimeMillis();
    final long oneDayBefore = baseTime - 86399000;
    TimeUnit.SECONDS.sleep(3);
    Assert.assertEquals(TimeUtils.daysEscapedOver(baseTime), 0);
    Assert.assertEquals(TimeUtils.daysEscapedOver(oneDayBefore), 1);
  }

  @Test
  public void testFormatInISOOffsetDateTime() {
    // Format timestamp in UTC
    Assert.assertEquals(TimeUtils.formatInISOOffsetDateTime(1607476982000L, "Z"),
        "2020-12-09T01:23:02Z");

    // Format timestamp in some time zone (PST)
    Assert.assertEquals(TimeUtils.formatInISOOffsetDateTime(1607477429000L, "-08:00"),
        "2020-12-08T17:30:29-08:00");

    // This should throw a DateTimeException exception since -38 is not in valid range (-18 to 18)
    assertThatThrownBy(() -> {
      TimeUtils.formatInISOOffsetDateTime(1607477429000L, "-38:00");
    }).isInstanceOf(DateTimeException.class);

    // This should throw a DateTimeException exception since "PST" isn't a valid zone offset
    assertThatThrownBy(() -> {
      TimeUtils.formatInISOOffsetDateTime(1607477429000L, "PST");
    }).isInstanceOf(DateTimeException.class);
  }

}
