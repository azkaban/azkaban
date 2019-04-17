/*
 * Copyright 2014-2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.jobtype.javautils;

import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.lang.StringUtils;

import azkaban.utils.Props;


public class ValidationUtils {

  public static void validateNotEmpty(String s, String name) {
    if (StringUtils.isEmpty(s)) {
      throw new IllegalArgumentException(name + " cannot be empty.");
    }
  }

  /**
   * Validates if all of the keys exist of none of them exist
   *
   * @throws IllegalArgumentException only if some of the keys exist
   */
  public static void validateAllOrNone(Props props, String... keys) {
    Objects.requireNonNull(keys);

    boolean allExist = true;
    boolean someExist = false;
    for (String key : keys) {
      Object val = props.get(key);
      allExist &= val != null;
      someExist |= val != null;
    }

    if (someExist && !allExist) {
      throw new IllegalArgumentException(
          "Either all of properties exist or none of them should exist for " + Arrays
              .toString(keys));
    }
  }

  /**
   * Validates all keys present in props
   */
  public static void validateAllNotEmpty(Props props, String... keys) {
    for (String key : keys) {
      props.getString(key);
    }
  }

  public static void validateAtleastOneNotEmpty(Props props, String... keys) {
    boolean exist = false;
    for (String key : keys) {
      Object val = props.get(key);
      exist |= val != null;
    }
    if (!exist) {
      throw new IllegalArgumentException(
          "At least one of these keys should exist " + Arrays.toString(keys));
    }
  }

  public static void validateSomeValuesNotEmpty(int notEmptyVals, String... vals) {
    int count = 0;
    for (String val : vals) {
      if (!StringUtils.isEmpty(val)) {
        count++;
      }
    }
    if (count != notEmptyVals) {
      throw new IllegalArgumentException(
          "Number of not empty vals " + count + " is not desired number " + notEmptyVals);
    }
  }
}
