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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

  public static final String DATE_TIME_ZONE_PATTERN = "yyyy/MM/dd HH:mm:ss z";
  public static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

  /**
   * Formats the given millisecond instant into a string using the pattern "yyyy/MM/dd HH:mm:ss z"
   */
  public static String formatDateTimeZone(final long timestampMs) {
    return format(timestampMs, DATE_TIME_ZONE_PATTERN);
  }

  /**
   * Formats the given millisecond instant into a string using the pattern "yyyy-MM-dd HH:mm:ss"
   */
  public static String formatDateTime(final long timestampMs) {
    return format(timestampMs, DATE_TIME_PATTERN);
  }

  private static String format(final long timestampMs, final String pattern) {
    if (timestampMs < 0) {
      return "-";
    }
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
    final ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampMs),
        ZoneId.systemDefault());
    return formatter.format(zonedDateTime);
  }

}
