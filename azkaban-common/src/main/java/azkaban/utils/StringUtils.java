/*
 * Copyright 2012 LinkedIn Corp.
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

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;


public class StringUtils {

  public static final char SINGLE_QUOTE = '\'';
  public static final char DOUBLE_QUOTE = '\"';
  private static final Pattern BROWSWER_PATTERN = Pattern
      .compile(".*Gecko.*|.*AppleWebKit.*|.*Trident.*|.*Chrome.*");

  public static String shellQuote(final String s, final char quoteCh) {
    final StringBuffer buf = new StringBuffer(s.length() + 2);

    buf.append(quoteCh);
    for (int i = 0; i < s.length(); i++) {
      final char ch = s.charAt(i);
      if (ch == quoteCh) {
        buf.append('\\');
      }
      buf.append(ch);
    }
    buf.append(quoteCh);

    return buf.toString();
  }

  @Deprecated
  public static String join(final List<String> list, final String delimiter) {
    final StringBuffer buffer = new StringBuffer();
    for (final String str : list) {
      buffer.append(str);
      buffer.append(delimiter);
    }

    return buffer.toString();
  }

  /**
   * Use this when you don't want to include Apache Common's string for plugins.
   */
  public static String join(final Collection<String> list, final String delimiter) {
    final StringBuffer buffer = new StringBuffer();
    for (final String str : list) {
      buffer.append(str);
      buffer.append(delimiter);
    }

    return buffer.toString();
  }

  /**
   * Don't bother to add delimiter for last element
   *
   * @return String - elements in the list separated by delimiter
   */
  public static String join2(final Collection<String> list, final String delimiter) {
    final StringBuffer buffer = new StringBuffer();
    boolean first = true;
    for (final String str : list) {
      if (!first) {
        buffer.append(delimiter);
      }
      buffer.append(str);
      first = false;

    }

    return buffer.toString();
  }

  public static boolean isFromBrowser(final String userAgent) {
    if (userAgent == null) {
      return false;
    }

    if (BROWSWER_PATTERN.matcher(userAgent).matches()) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean isEmpty(final String value) {
    if (value == null) {
      return true;
    }
    return  (value.trim().isEmpty());
  }

}
