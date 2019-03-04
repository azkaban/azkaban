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
 *
 */

package azkaban.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import azkaban.execapp.metric.ProjectCacheHitRatio;
import org.junit.Test;


public class ProjectCacheHitRatioTest {

  @Test
  public void testProjectCacheZeroHit() {
    //given
    final ProjectCacheHitRatio hitRatio = new ProjectCacheHitRatio();

    //when zero hit and zero miss then
    assertThat(hitRatio.getRatio().getValue()).isEqualTo(Double.NaN);

    //when
    hitRatio.markMiss();
    //then
    assertThat(hitRatio.getRatio().getValue()).isEqualTo(0);
  }

  @Test
  public void testProjectCacheMetricsHit() throws InterruptedException {
    //given
    final ProjectCacheHitRatio hitRatio = new ProjectCacheHitRatio();

    //when one hit
    hitRatio.markHit();
    //then
    assertThat(hitRatio.getRatio().getValue()).isEqualTo(1);

    //when one miss
    hitRatio.markMiss();
    //then
    assertThat(hitRatio.getRatio().getValue()).isEqualTo(0.5);
  }
}
