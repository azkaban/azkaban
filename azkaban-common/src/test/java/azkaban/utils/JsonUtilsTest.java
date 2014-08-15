/*
 * Copyright 2014 LinkedIn Corp.
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

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class JsonUtilsTest {
  @Test
  public void writePropsNoJarDependencyTest1() throws IOException {
    Map<String, String> test = new HashMap<String, String>();
    test.put("\"myTest\n\b", "myValue\t\\");
    test.put("normalKey", "Other key");

    StringWriter writer = new StringWriter();
    JSONUtils.writePropsNoJarDependency(test, writer);

    String jsonStr = writer.toString();
    System.out.println(writer.toString());

    @SuppressWarnings("unchecked")
    Map<String, String> result =
        (Map<String, String>) JSONUtils.parseJSONFromString(jsonStr);
    checkInAndOut(test, result);
  }

  @Test
  public void writePropsNoJarDependencyTest2() throws IOException {
    Map<String, String> test = new HashMap<String, String>();
    test.put("\"myTest\n\b", "myValue\t\\");

    StringWriter writer = new StringWriter();
    JSONUtils.writePropsNoJarDependency(test, writer);

    String jsonStr = writer.toString();
    System.out.println(writer.toString());

    @SuppressWarnings("unchecked")
    Map<String, String> result =
        (Map<String, String>) JSONUtils.parseJSONFromString(jsonStr);
    checkInAndOut(test, result);
  }

  private static void checkInAndOut(Map<String, String> before,
      Map<String, String> after) {
    for (Map.Entry<String, String> entry : before.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      String retValue = after.get(key);
      Assert.assertEquals(value, retValue);
    }
  }

}
