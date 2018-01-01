/*
 * Copyright 2018 LinkedIn Corp.
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

public class UtilsTest {

  @Test
  public void testDeprecatedFormatDuration() {
    Assert.assertEquals("-", Utils.formatDuration(-1, 999));
    Assert.assertEquals("0 sec", Utils.formatDuration(0, 999));
    Assert.assertEquals("1 sec", Utils.formatDuration(0, 1000));
    Assert.assertEquals("1m 0s", Utils.formatDuration(0, 60000));
  }

  @Test
  public void testFormatDurationSI() {
    Assert.assertEquals("0ms", Utils.formatDurationSI(0));
    Assert.assertEquals("999ms", Utils.formatDurationSI(999));
    Assert.assertEquals("1s", Utils.formatDurationSI(1_000));
    Assert.assertEquals("59s", Utils.formatDurationSI(59_999));
    Assert.assertEquals("1min 0s", Utils.formatDurationSI(60_000));
    Assert.assertEquals("59min 59s", Utils.formatDurationSI(3_599_999));
    Assert.assertEquals("1h 0min 0s", Utils.formatDurationSI(3_600_000));
    Assert.assertEquals("23h 59min 59s", Utils.formatDurationSI(24 * 3_600_000 - 1));
    Assert.assertEquals("1d 0h 0min", Utils.formatDurationSI(24 * 3_600_000));
    Assert.assertEquals("10d 0h 0min", Utils.formatDurationSI(240 * 3_600_000));
  }
}