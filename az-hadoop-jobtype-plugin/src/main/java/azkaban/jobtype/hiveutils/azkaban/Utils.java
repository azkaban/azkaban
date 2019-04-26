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
package azkaban.jobtype.hiveutils.azkaban;

import java.util.Collection;
import java.util.Properties;


public class Utils {

  private Utils() {

  }

  public static String joinNewlines(Collection<String> strings) {
    if (strings == null || strings.size() == 0) {
      return null;
    }

    StringBuilder sb = new StringBuilder();

    for (String s : strings) {
      String trimmed = s.trim();
      sb.append(trimmed);
      if (!trimmed.endsWith("\n")) {
        sb.append("\n");
      }
    }

    return sb.toString();
  }

  // Hey, look! It's this method again! It's the freaking Where's Waldo of
  // methods...
  public static String verifyProperty(Properties p, String key)
      throws HiveViaAzkabanException {
    String value = p.getProperty(key);
    if (value == null) {
      throw new HiveViaAzkabanException("Can't find property " + key
          + " in provided Properties. Bailing");
    }
    // TODO: Add a log entry here for the value
    return value;
  }
}
