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

package azkaban.reportal.util.tableau;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.Test;

public class CountDownTest {

  @Test
  public void testMoreTimeRemaining() throws InterruptedException {
    final Countdown countDown = new Countdown(Duration.ofMinutes(1));
    assertThat(countDown.moreTimeRemaining()).isTrue();
    countDown.countDownByOneMinute();
    assertThat(countDown.moreTimeRemaining()).isFalse();
  }
}
