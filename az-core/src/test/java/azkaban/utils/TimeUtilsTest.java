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

import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;


public class TimeUtilsTest {

  @Test
  public void testTimeEscapedOver() throws InterruptedException {
    long baseTime = System.currentTimeMillis();
    TimeUnit.SECONDS.sleep(1);
    Assert.assertTrue( TimeUtils.timeEscapedOver(baseTime, 1));
    Assert.assertFalse(TimeUtils.timeEscapedOver(baseTime, 2));
  }

  @Test
  public void testDayEscapedOver() throws InterruptedException {
    long baseTime = System.currentTimeMillis();
    long oneDayBefore =  baseTime - 86399000;
    TimeUnit.SECONDS.sleep(3);
    Assert.assertEquals( TimeUtils.daysEscapedOver(baseTime), 0);
    Assert.assertEquals(TimeUtils.daysEscapedOver(oneDayBefore), 1);
  }

}
