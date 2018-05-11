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

package azkaban.dag;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class StatusTest {

  //todo HappyRay: add tests for other status
  @Test
  public void disabled_is_terminal() {
    assertThat(Status.DISABLED.isTerminal()).isTrue();
  }

  @Test
  public void running_is_not_terminal() {
    assertThat(Status.RUNNING.isTerminal()).isFalse();
  }

  @Test
  public void diabled_is_effectively_success() {
    assertThat(Status.DISABLED.isSuccessEffectively()).isTrue();
  }

  @Test
  public void failure_is_not_effectively_success() {
    assertThat(Status.FAILURE.isSuccessEffectively()).isFalse();
  }

}
