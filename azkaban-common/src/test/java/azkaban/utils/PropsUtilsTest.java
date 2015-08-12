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
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class PropsUtilsTest {
  @Test
  public void testGoodResolveProps() throws IOException {
    Props propsGrandParent = new Props();
    Props propsParent = new Props(propsGrandParent);
    Props props = new Props(propsParent);

    // Testing props in general
    props.put("letter", "a");
    propsParent.put("letter", "b");
    propsGrandParent.put("letter", "c");

    Assert.assertEquals("a", props.get("letter"));
    propsParent.put("my", "name");
    propsParent.put("your", "eyes");
    propsGrandParent.put("their", "ears");
    propsGrandParent.put("your", "hair");

    Assert.assertEquals("name", props.get("my"));
    Assert.assertEquals("eyes", props.get("your"));
    Assert.assertEquals("ears", props.get("their"));

    props.put("res1", "${my}");
    props.put("res2", "${their} ${letter}");
    props.put("res7", "${my} ${res5}");

    propsParent.put("res3", "${your} ${their} ${res4}");
    propsGrandParent.put("res4", "${letter}");
    propsGrandParent.put("res5", "${their}");
    propsParent.put("res6", " t ${your} ${your} ${their} ${res5}");

    Props resolved = PropsUtils.resolveProps(props);
    Assert.assertEquals("name", resolved.get("res1"));
    Assert.assertEquals("ears a", resolved.get("res2"));
    Assert.assertEquals("eyes ears a", resolved.get("res3"));
    Assert.assertEquals("a", resolved.get("res4"));
    Assert.assertEquals("ears", resolved.get("res5"));
    Assert.assertEquals(" t eyes eyes ears ears", resolved.get("res6"));
    Assert.assertEquals("name ears", resolved.get("res7"));
  }

  @Test
  public void testInvalidSyntax() throws Exception {
    Props propsGrandParent = new Props();
    Props propsParent = new Props(propsGrandParent);
    Props props = new Props(propsParent);

    propsParent.put("my", "name");
    props.put("res1", "$(my)");

    Props resolved = PropsUtils.resolveProps(props);
    Assert.assertEquals("$(my)", resolved.get("res1"));
  }

  @Test
  public void testExpressionResolution() throws IOException {
    Props props =
        Props.of("normkey", "normal", "num1", "1", "num2", "2", "num3", "3",
            "variablereplaced", "${num1}", "expression1", "$(1+10)",
            "expression2", "$(1+10)*2", "expression3",
            "$($(${num1} + ${num3})*10)", "expression4",
            "$(${num1} + ${expression3})", "expression5",
            "$($($(2+3)) + 3) + $(${expression3} + 1)", "expression6",
            "$(1 + ${normkey})", "expression7", "$(\"${normkey}\" + 1)",
            "expression8", "${expression1}", "expression9", "$((2+3) + 3)");

    Props resolved = PropsUtils.resolveProps(props);
    Assert.assertEquals("normal", resolved.get("normkey"));
    Assert.assertEquals("1", resolved.get("num1"));
    Assert.assertEquals("2", resolved.get("num2"));
    Assert.assertEquals("3", resolved.get("num3"));
    Assert.assertEquals("1", resolved.get("variablereplaced"));
    Assert.assertEquals("11", resolved.get("expression1"));
    Assert.assertEquals("11*2", resolved.get("expression2"));
    Assert.assertEquals("40", resolved.get("expression3"));
    Assert.assertEquals("41", resolved.get("expression4"));
    Assert.assertEquals("8 + 41", resolved.get("expression5"));
    Assert.assertEquals("1", resolved.get("expression6"));
    Assert.assertEquals("normal1", resolved.get("expression7"));
    Assert.assertEquals("11", resolved.get("expression8"));
    Assert.assertEquals("8", resolved.get("expression9"));
  }

  @Test
  public void testMalformedExpressionProps() throws IOException {
    // unclosed
    Props props = Props.of("key", "$(1+2");
    failIfNotException(props);

    props = Props.of("key", "$((1+2)");
    failIfNotException(props);

    // bad variable replacement
    props = Props.of("key", "$(${dontexist}+2)");
    failIfNotException(props);

    // bad expression
    props = Props.of("key", "$(2 +)");
    failIfNotException(props);

    // bad expression
    props = Props.of("key", "$(2 + #hello)");
    failIfNotException(props);
  }

  @Test
  public void testGetFlattenedProps() throws Exception {

    // for empty props empty flattened map is expected to be returned.
    Props grandParentProps = new Props();
    Assert.assertTrue(grandParentProps.getFlattened().isEmpty());

    // single level
    grandParentProps.put("test1","value1");
    grandParentProps.put("test2","value2");
    Map<String,String> set = grandParentProps.getFlattened();
    Assert.assertEquals(2,set.size());
    Assert.assertEquals("value1", set.get("test1"));
    Assert.assertEquals("value2", set.get("test2"));

    // multiple levels .
    Props parentProps = new Props(grandParentProps);
    parentProps.put("test3","value3");
    parentProps.put("test4","value4");
    set = parentProps.getFlattened();
    Assert.assertEquals(4,set.size());
    Assert.assertEquals("value3", set.get("test3"));
    Assert.assertEquals("value1", set.get("test1"));

    // multiple levels with same keys  .
    Props props = new Props(parentProps);
    props.put("test5","value5");
    props.put("test1","value1.1");
    set = props.getFlattened();
    Assert.assertEquals(5,set.size());
    Assert.assertEquals("value5", set.get("test5"));
    Assert.assertEquals("value1.1", set.get("test1"));

    // verify when iterating the elements are sorted by the key value.
    Props props2 = new Props();
    props2.put("2","2");
    props2.put("0","0");
    props2.put("1","1");
    set = props2.getFlattened();
    int index = 0 ;
    for (Map.Entry<String, String> item : set.entrySet())
    {
      Assert.assertEquals(item.getKey(),Integer.toString(index++));
    }
  }

  @Test
  public void testCyclesResolveProps() throws IOException {
    Props propsGrandParent = new Props();
    Props propsParent = new Props(propsGrandParent);
    Props props = new Props(propsParent);

    // Testing props in general
    props.put("a", "${a}");
    failIfNotException(props);

    props.put("a", "${b}");
    props.put("b", "${a}");
    failIfNotException(props);

    props.clearLocal();
    props.put("a", "${b}");
    props.put("b", "${c}");
    propsParent.put("d", "${a}");
    failIfNotException(props);

    props.clearLocal();
    props.put("a", "testing ${b}");
    props.put("b", "${c}");
    propsGrandParent.put("c", "${d}");
    propsParent.put("d", "${a}");
    failIfNotException(props);

    props.clearLocal();
    props.put("a", "testing ${c} ${b}");
    props.put("b", "${c} test");
    propsGrandParent.put("c", "${d}");
    propsParent.put("d", "${a}");
    failIfNotException(props);
  }

  private void failIfNotException(Props props) {
    try {
      PropsUtils.resolveProps(props);
      Assert.fail();
    } catch (UndefinedPropertyException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    }
  }
}
