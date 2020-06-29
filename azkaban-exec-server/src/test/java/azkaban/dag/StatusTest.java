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

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class StatusTest {

  @Test
  public void is_terminal() {
    // given
    final Map<Status, Boolean> isStatusTerminalMap = new HashMap<>();
    isStatusTerminalMap.put(Status.BLOCKED, Boolean.FALSE);
    isStatusTerminalMap.put(Status.CANCELED, Boolean.TRUE);
    isStatusTerminalMap.put(Status.DISABLED, Boolean.TRUE);
    isStatusTerminalMap.put(Status.FAILURE, Boolean.TRUE);
    isStatusTerminalMap.put(Status.KILLED, Boolean.TRUE);
    isStatusTerminalMap.put(Status.KILLING, Boolean.FALSE);
    isStatusTerminalMap.put(Status.READY, Boolean.FALSE);
    isStatusTerminalMap.put(Status.RUNNING, Boolean.FALSE);
    isStatusTerminalMap.put(Status.SUCCESS, Boolean.TRUE);

    assertMapSizeMatchEnumSize(isStatusTerminalMap);
    for (final Map.Entry<Status, Boolean> entry : isStatusTerminalMap.entrySet()) {
      final Status key = entry.getKey();
      assertThat(key.isTerminal()).as("key: %s", key).isEqualTo(entry.getValue());
    }
  }

  @Test
  public void is_effectively_success() {
    // given
    final Map<Status, Boolean> isStatusEffectivelySuccess = new HashMap<>();
    isStatusEffectivelySuccess.put(Status.BLOCKED, Boolean.FALSE);
    isStatusEffectivelySuccess.put(Status.CANCELED, Boolean.FALSE);
    isStatusEffectivelySuccess.put(Status.DISABLED, Boolean.TRUE);
    isStatusEffectivelySuccess.put(Status.FAILURE, Boolean.FALSE);
    isStatusEffectivelySuccess.put(Status.KILLED, Boolean.FALSE);
    isStatusEffectivelySuccess.put(Status.KILLING, Boolean.FALSE);
    isStatusEffectivelySuccess.put(Status.READY, Boolean.FALSE);
    isStatusEffectivelySuccess.put(Status.RUNNING, Boolean.FALSE);
    isStatusEffectivelySuccess.put(Status.SUCCESS, Boolean.TRUE);

    assertMapSizeMatchEnumSize(isStatusEffectivelySuccess);
    for (final Map.Entry<Status, Boolean> entry : isStatusEffectivelySuccess.entrySet()) {
      final Status key = entry.getKey();
      assertThat(key.isSuccessEffectively()).as("key: %s", key).isEqualTo(entry.getValue());
    }
  }

  @Test
  public void is_pre_run_state() {
    // given
    final Map<Status, Boolean> isPrerunStateMap = new HashMap<>();
    isPrerunStateMap.put(Status.BLOCKED, Boolean.TRUE);
    isPrerunStateMap.put(Status.CANCELED, Boolean.FALSE);
    isPrerunStateMap.put(Status.DISABLED, Boolean.TRUE);
    isPrerunStateMap.put(Status.FAILURE, Boolean.FALSE);
    isPrerunStateMap.put(Status.KILLED, Boolean.FALSE);
    isPrerunStateMap.put(Status.KILLING, Boolean.FALSE);
    isPrerunStateMap.put(Status.READY, Boolean.TRUE);
    isPrerunStateMap.put(Status.RUNNING, Boolean.FALSE);
    isPrerunStateMap.put(Status.SUCCESS, Boolean.FALSE);

    assertMapSizeMatchEnumSize(isPrerunStateMap);
    for (final Map.Entry<Status, Boolean> entry : isPrerunStateMap.entrySet()) {
      final Status key = entry.getKey();
      assertThat(key.isPreRunState()).as("key: %s", key).isEqualTo(entry.getValue());
    }
  }

  /**
   * Asserts the given map contains the same number of entries as the number of values of the {@link
   * Status} has.
   *
   * @param map a map that contains status and its associated boolean value
   */
  private void assertMapSizeMatchEnumSize(
      final Map<Status, Boolean> map) {
    final int mapSize = map.size();
    final int enumSize = Status.values().length;
    assertThat(enumSize).isEqualTo(mapSize);
  }
}
