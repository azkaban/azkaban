/*
* Copyright 2018 LinkedIn Corp.
*
* Licensed under the Apache License, Version 2.0 (the “License”); you may not
* use this file except in compliance with the License. You may obtain a copy of
* the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an “AS IS” BASIS, WITHOUT
* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
* License for the specific language governing permissions and limitations under
* the License.
*/
package azkaban.utils;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.Test;

public class CaseInsensitiveConcurrentHashMapTest {

  private static final String LOWER_KEY = "test_key";
  private static final String UPPER_KEY = "TEST_KEY";
  private static final String WRONG_KEY = "wrong_key";
  private static final Object VALUE_1 = new Object();
  private static final Object VALUE_2 = new Object();
  private final CaseInsensitiveConcurrentHashMap map = new
      CaseInsensitiveConcurrentHashMap();

  @Test
  public void testPut() {
    this.map.put(LOWER_KEY, VALUE_1);
    assertThat(this.map.containsKey(LOWER_KEY)).isTrue();
    assertThat(this.map.containsKey(UPPER_KEY)).isTrue();
    assertThat(this.map.containsKey(WRONG_KEY)).isFalse();
  }

  @Test
  public void testGet() {
    this.map.put(LOWER_KEY, VALUE_1);
    this.map.put(UPPER_KEY, VALUE_2);
    assertThat(this.map.get(LOWER_KEY)).isEqualTo(VALUE_2);
    assertThat(this.map.get(UPPER_KEY)).isEqualTo(VALUE_2);
    assertThat(this.map.get(WRONG_KEY)).isNull();
  }

  @Test
  public void testRemove() {
    this.map.put(LOWER_KEY, VALUE_1);
    this.map.remove(UPPER_KEY);
    assertThat(this.map.containsKey(LOWER_KEY)).isFalse();
    this.map.put(UPPER_KEY, VALUE_2);
    this.map.remove(LOWER_KEY);
    assertThat(this.map.containsKey(UPPER_KEY)).isFalse();
    assertThat(this.map.remove(WRONG_KEY)).isNull();
  }

}
