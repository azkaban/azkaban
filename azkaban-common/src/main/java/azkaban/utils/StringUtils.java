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

  public static String shellQuote(String s, char quoteCh) {
    StringBuffer buf = new StringBuffer(s.length() + 2);

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
  public static String join(List<String> list, String delimiter) {
    StringBuffer buffer = new StringBuffer();
    for (String str : list) {
      buffer.append(str);
      buffer.append(delimiter);
    }

    return buffer.toString();
  }

  /**
   * Use this when you don't want to include Apache Common's string for plugins.
   *
   * @param list
   * @param delimiter
   * @return
   */
  public static String join(Collection<String> list, String delimiter) {
    StringBuffer buffer = new StringBuffer();
    for (String str : list) {
      buffer.append(str);
      buffer.append(delimiter);
    }

    return buffer.toString();
  }

  /**
   * Don't bother to add delimiter for last element
   * 
   * @param list
   * @param delimiter
   * @return String - elements in the list separated by delimiter
   */
  public static String join2(Collection<String> list, String delimiter) {
    StringBuffer buffer = new StringBuffer();
    boolean first = true;
    for (String str : list) {
      if (!first) {
        buffer.append(delimiter);
      }
      buffer.append(str);
      first = false;

    }

    return buffer.toString();
  }

  private static final Pattern BROWSWER_PATTERN = Pattern
      .compile(".*Gecko.*|.*AppleWebKit.*|.*Trident.*|.*Chrome.*");

  public static boolean isFromBrowser(String userAgent) {
    if (userAgent == null) {
      return false;
    }

    if (BROWSWER_PATTERN.matcher(userAgent).matches()) {
      return true;
    } else {
      return false;
    }
  }
}
