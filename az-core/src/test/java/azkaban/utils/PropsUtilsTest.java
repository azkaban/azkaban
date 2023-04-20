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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class PropsUtilsTest {

  @Test
  public void testGoodResolveProps() throws IOException {
    final Props propsGrandParent = new Props();
    final Props propsParent = new Props(propsGrandParent);
    final Props props = new Props(propsParent);

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

    final Props resolved = PropsUtils.resolveProps(props);
    Assert.assertEquals("name", resolved.get("res1"));
    Assert.assertEquals("ears a", resolved.get("res2"));
    Assert.assertEquals("eyes ears a", resolved.get("res3"));
    Assert.assertEquals("a", resolved.get("res4"));
    Assert.assertEquals("ears", resolved.get("res5"));
    Assert.assertEquals(" t eyes eyes ears ears", resolved.get("res6"));
    Assert.assertEquals("name ears", resolved.get("res7"));
  }

  @Test
  public void testResolveProps() throws IOException {
    final Props props = new Props();

    props.put("spark.version.sparky", "2.3.0");
    props.put("spark.version", "${spark.version.${spark.branch}}");
    props.put("spark.branch", "${test}");
    props.put("test", "sparky");
    props.put("B", "${A}");
    props.put("C", "${B}");
    final Props resolved = PropsUtils.resolveProps(props, true);
    Assert.assertEquals("${A}",resolved.get("B"));
    Assert.assertEquals("${A}", resolved.get("C"));
    Assert.assertEquals("2.3.0",resolved.get("spark.version"));
  }

  @Test
  public void testInvalidSyntax() throws Exception {
    final Props propsGrandParent = new Props();
    final Props propsParent = new Props(propsGrandParent);
    final Props props = new Props(propsParent);

    propsParent.put("my", "name");
    props.put("res1", "$(my)");

    final Props resolved = PropsUtils.resolveProps(props);
    Assert.assertEquals("$(my)", resolved.get("res1"));
  }

  @Test
  public void testExpressionResolution() throws IOException {
    final Props props =
        Props.of("normkey", "normal", "num1", "1", "num2", "2", "num3", "3",
            "variablereplaced", "${num1}", "expression1", "$(1+10)",
            "expression2", "$(1+10)*2", "expression3",
            "$($(${num1} + ${num3})*10)", "expression4",
            "$(${num1} + ${expression3})", "expression5",
            "$($($(2+3)) + 3) + $(${expression3} + 1)", "expression6",
            "$(1 + ${normkey})", "expression7", "$(\"${normkey}\" + 1)",
            "expression8", "${expression1}", "expression9", "$((2+3) + 3)");

    final Props resolved = PropsUtils.resolveProps(props);
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
    final Props grandParentProps = new Props();
    Assert.assertTrue(grandParentProps.getFlattened().isEmpty());

    // single level
    grandParentProps.put("test1", "value1");
    grandParentProps.put("test2", "value2");
    Map<String, String> set = grandParentProps.getFlattened();
    Assert.assertEquals(2, set.size());
    Assert.assertEquals("value1", set.get("test1"));
    Assert.assertEquals("value2", set.get("test2"));

    // multiple levels .
    final Props parentProps = new Props(grandParentProps);
    parentProps.put("test3", "value3");
    parentProps.put("test4", "value4");
    set = parentProps.getFlattened();
    Assert.assertEquals(4, set.size());
    Assert.assertEquals("value3", set.get("test3"));
    Assert.assertEquals("value1", set.get("test1"));

    // multiple levels with same keys  .
    final Props props = new Props(parentProps);
    props.put("test5", "value5");
    props.put("test1", "value1.1");
    set = props.getFlattened();
    Assert.assertEquals(5, set.size());
    Assert.assertEquals("value5", set.get("test5"));
    Assert.assertEquals("value1.1", set.get("test1"));

    // verify when iterating the elements are sorted by the key value.
    final Props props2 = new Props();
    props2.put("2", "2");
    props2.put("0", "0");
    props2.put("1", "1");
    set = props2.getFlattened();
    int index = 0;
    for (final Map.Entry<String, String> item : set.entrySet()) {
      Assert.assertEquals(item.getKey(), Integer.toString(index++));
    }
  }

  @Test
  public void testCyclesResolveProps() throws IOException {
    final Props propsGrandParent = new Props();
    final Props propsParent = new Props(propsGrandParent);
    final Props props = new Props(propsParent);

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

  @Test
  public void testGetPropertyDiff() throws IOException {
    final Props oldProps = new Props();
    final Props newProps1 = new Props();

    oldProps.put("a", "a_value1");
    oldProps.put("b", "b_value1");

    newProps1.put("b", "b_value2");

    final String message1 = PropsUtils.getPropertyDiff(oldProps, newProps1);
    Assert.assertEquals(message1,
        "Deleted Properties: [ a, a_value1], \nModified Properties: [ b, b_value1-->b_value2], ");

    final Props newProps2 = new Props();

    newProps2.put("a", "a_value1");
    newProps2.put("b", "b_value1");
    newProps2.put("c", "c_value1");

    final String message2 = PropsUtils.getPropertyDiff(oldProps, newProps2);
    Assert.assertEquals(message2, "Newly created Properties: [ c, c_value1], \n");

    final Props newProps3 = new Props();

    newProps3.put("b", "b_value1");
    newProps3.put("c", "a_value1");

    final String message3 = PropsUtils.getPropertyDiff(oldProps, newProps3);
    Assert.assertEquals(message3,
        "Newly created Properties: [ c, a_value1], \nDeleted Properties: [ a, a_value1], \n");
  }

  private void failIfNotException(final Props props) {
    try {
      PropsUtils.resolveProps(props);
      Assert.fail();
    } catch (final UndefinedPropertyException e) {
      e.printStackTrace();
    } catch (final IllegalArgumentException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testNewPropsForFileNotExist() throws IOException {
    File file = Mockito.mock(File.class);
    Mockito.when(file.exists()).thenReturn(false);
    Props parent = new Props().setSource("test");
    Props props = PropsUtils.newProps(parent, file);
    Assert.assertNotNull(props);
    Assert.assertNotNull(props.getParent());
    Assert.assertEquals("test", props.getParent().getSource());
  }

  @Test
  public void testNewPropsForNullParentAndFileNotExist() throws IOException {
    File file = Mockito.mock(File.class);
    Mockito.when(file.exists()).thenReturn(false);
    Props props = PropsUtils.newProps(null, file);
    Assert.assertNull(props);
  }


  @Test
  public void testPropsWithAllPropertiesDefined() {
    Props props = new Props();
    String valA = "a";
    props.put("A", valA);
    props.put("B", "${A}");
    props.put("C", "c");
    Props resolvedProps = PropsUtils.resolveProps(props, true);
    Assert.assertEquals(valA, resolvedProps.get("B"));
    Assert.assertEquals(valA, resolvedProps.get("A"));
    Assert.assertEquals("c", resolvedProps.get("C"));
  }

  @Test
  public void testPropsWithMultipleReferencesToDefinedProps() {
    Props props = new Props();
    String valA = "a";
    props.put("A", valA);
    String valB = "B";
    props.put("B", valB);
    props.put("C", "${A}${B}");
    Props resolvedProps = PropsUtils.resolveProps(props, true);
    Assert.assertEquals(valA, resolvedProps.get("A"));
    Assert.assertEquals(valB, resolvedProps.get("B"));
    Assert.assertEquals(valA + valB, resolvedProps.get("C"));
  }

  @Test
  public void testPropsWithNestedDefinedProperties() {
    Props props = new Props();
    String valA = "a";
    props.put("A", valA);
    props.put("B", "${A}");
    props.put("C", "${B}");
    Props resolvedProps = PropsUtils.resolveProps(props, true);
    Assert.assertEquals(valA, resolvedProps.get("B"));
    Assert.assertEquals(valA, resolvedProps.get("A"));
    Assert.assertEquals(valA, resolvedProps.get("C"));
  }

  @Test
  public void testPropsWithPropertiesUndefined() {
    Props props = new Props();
    props.put("B", "${A}");
    props.put("C", "c");
    Props resolvedProps = PropsUtils.resolveProps(props, true);
    Assert.assertNull(resolvedProps.get("A"));
    Assert.assertEquals("${A}", resolvedProps.get("B"));
    Assert.assertEquals("c", resolvedProps.get("C"));
  }

  @Test
  public void testPropsWithNestedPropertiesUndefined() {
    Props props = new Props();
    props.put("B", "${A}");
    props.put("C", "${B}");
    Props resolvedProps = PropsUtils.resolveProps(props, true);
    Assert.assertEquals("${A}",resolvedProps.get("B"));
    Assert.assertEquals("${A}", resolvedProps.get("C"));
  }
}
