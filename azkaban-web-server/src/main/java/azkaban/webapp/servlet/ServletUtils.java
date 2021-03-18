/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.webapp.servlet;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class containing static methods to be used by various Servlets
 */
public class ServletUtils {

  /**
   * Returns the Set of String from the comma-separated val
   */
  public static Set<String> getSetFromString(final String val) {
    if (val == null || val.trim().length() == 0) {
      return Collections.emptySet();
    }
    return new HashSet<>(Arrays.asList(val.split("\\s*,\\s*")));
  }
}
