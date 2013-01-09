/*
 * Copyright 2012 LinkedIn, Inc
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

public class StringUtils
{
  public static final char SINGLE_QUOTE = '\'';
  public static final char DOUBLE_QUOTE = '\"';
  
  public static String shellQuote(String s, char quoteCh)
  {
    StringBuffer buf = new StringBuffer(s.length()+2);

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
}
