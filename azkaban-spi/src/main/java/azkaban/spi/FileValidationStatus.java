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

package azkaban.spi;

import java.util.HashMap;
import java.util.Map;


// REMOVED = validator removed this file, it is blacklisted
// VALID = validator gave this file the green light - no modifications made, it's fine as is.
// NEW = not yet processed by the validator
public enum FileValidationStatus {
  REMOVED(0), VALID(1), NEW(2);

  private final int value;
  private static Map map = new HashMap<>();

  FileValidationStatus(final int newValue) {
    value = newValue;
  }

  static {
    for (FileValidationStatus v : FileValidationStatus.values()) {
      map.put(v.value, v);
    }
  }

  public static FileValidationStatus valueOf(final int v) {
    return (FileValidationStatus) map.get(v);
  }

  public int getValue() { return value; }
}
