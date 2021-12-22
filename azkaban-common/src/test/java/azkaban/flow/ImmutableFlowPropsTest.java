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
package azkaban.flow;

import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Test;


public class ImmutableFlowPropsTest {

  // Tests FlowProps Factory: createFlowProps(final String, final String).
  // Checks that the objects created by this factory are interned whenever possible.
  @Test
  public void testThatFactoryOneDoesIntern() throws Exception {
    final String strAbcd1 = new String("A_B_C_D");
    final String strAbcd2 = new String("A_B_C_D");

    final String strXyz1 = new String("X_Y_Z");
    final String strXyz2 = new String("X_Y_Z");

    final ImmutableFlowProps immutableFlowProps1 = ImmutableFlowProps
        .createFlowProps(strAbcd1, strXyz1);
    final ImmutableFlowProps immutableFlowProps2 = ImmutableFlowProps
        .createFlowProps(strAbcd2, strXyz2);

    // Check that both the FlowProps refer to the same object, when the string contents are the
    // same in spite of the str-references that were passed to their constructors being different.
    // i.e.
    // 1. strAbcd1 and strAbcd2 should be different instance of String
    // 2. strXyz1 and strXyz2 should be different instance of String
    // 3. immutableFlowProps1 and immutableFlowProps2 should be referencing to the same object.
    //    Creation of the 2nd object must have returned the reference to the first one because of
    //    the underlying interning.

    // Refer http://errorprone.info/bugpattern/ReferenceEquality for meaning of assertNotSame and
    // why we could not instead use operators '==' or '!=' over here.
    Assert.assertNotSame("strAbcd1 and strAbcd2 should be different instance of String",
        strAbcd1, strAbcd2);
    Assert.assertNotSame("strXyz1 and strXyz2 should be different instance of String",
        strXyz1, strXyz2);

    Assert.assertTrue(immutableFlowProps1.equals(immutableFlowProps2));
    Assert.assertSame("Both the FlowProps should refer to the same object because of inbuilt "
            + "interning.", immutableFlowProps1, immutableFlowProps2);
  }

  // Tests FlowProps Factory: createFlowProps(final String, final String).
  // Checks that the objects created by this factory are distinct when not equivalent.
  @Test
  public void testThatFactoryOneDoesNotIntern1() throws Exception {
    final String strAbcd1 = new String("A_B_C_D");
    final String strAbcd2 = null;

    final String strXyz1 = new String("X_Y_Z");
    final String strXyz2 = new String("X_Y_Z");

    final ImmutableFlowProps immutableFlowProps1 = ImmutableFlowProps
        .createFlowProps(strAbcd1, strXyz1);
    final ImmutableFlowProps immutableFlowProps2 = ImmutableFlowProps
        .createFlowProps(strAbcd2, strXyz2);

    // Check that both the FlowProps refer to the same object, when the string contents are the
    // same in spite of the str-references that were passed to their constructors being different.
    // i.e.
    // 1. strAbcd1 and strAbcd2 should be different instance of String
    // 2. strXyz1 and strXyz2 should be different instance of String
    // 3. immutableFlowProps1 and immutableFlowProps2 should be referencing different object.


    // Refer http://errorprone.info/bugpattern/ReferenceEquality for meaning of assertNotSame and
    // why we could not instead use operators '==' or '!=' over here.
    Assert.assertNotSame("strAbcd1 and strAbcd2 should be different instance of String",
        strAbcd1, strAbcd2);
    Assert.assertNotSame("strXyz1 and strXyz2 should be different instance of String",
        strXyz1, strXyz2);

    Assert.assertFalse(immutableFlowProps1.equals(immutableFlowProps2));
    Assert.assertNotSame("Both the FlowProps should refer to the different objects.",
        immutableFlowProps1, immutableFlowProps2);
  }

  // Tests FlowProps Factory: createFlowProps(final String, final String).
  // Checks that the objects created by this factory are distinct when not equivalent.
  @Test
  public void testThatFactoryOneDoesNotIntern2() throws Exception {
    final String strAbcd1 = new String("A_B_C_D");
    final String strAbcd2 = new String("A_B_C_D");

    final String strXyz1 = new String("X_Y_Z");
    final String strXyz2 = new String("X_Y");

    final ImmutableFlowProps immutableFlowProps1 = ImmutableFlowProps
        .createFlowProps(strAbcd1, strXyz1);
    final ImmutableFlowProps immutableFlowProps2 = ImmutableFlowProps
        .createFlowProps(strAbcd2, strXyz2);

    // Check that both the FlowProps refer to the same object, when the string contents are the
    // same in spite of the str-references that were passed to their constructors being different.
    // i.e.
    // 1. strAbcd1 and strAbcd2 should be different instance of String
    // 2. strXyz1 and strXyz2 should be different instance of String
    // 3. immutableFlowProps1 and immutableFlowProps2 should be referencing different object.


    // Refer http://errorprone.info/bugpattern/ReferenceEquality for meaning of assertNotSame and
    // why we could not instead use operators '==' or '!=' over here.
    Assert.assertNotSame("strAbcd1 and strAbcd2 should be different instance of String",
        strAbcd1, strAbcd2);
    Assert.assertNotSame("strXyz1 and strXyz2 should be different instance of String",
        strXyz1, strXyz2);

    Assert.assertFalse(immutableFlowProps1.equals(immutableFlowProps2));
    Assert.assertNotSame("Both the FlowProps should refer to the different objects.",
        immutableFlowProps1, immutableFlowProps2);
  }

  // Tests FlowProps Builder: createFlowProps(final Props).
  // Checks that the objects created by this builder are interned whenever possible.
  @Test
  public void testThatFactoryTwoDoesIntern() throws Exception {
    final Props props = new Props();

    final ImmutableFlowProps flowProps1 = ImmutableFlowProps.createFlowProps(props);
    final ImmutableFlowProps flowProps2 = ImmutableFlowProps.createFlowProps(props);

    Assert.assertSame("Both the FlowProps should refer to the same object because of inbuilt "
        + "interning.", flowProps1, flowProps2);
  }

  // Tests FlowProps Builder: createFlowProps(final Props).
  @Test
  public void testThatFactoryTwoDoesNotIntern() throws Exception {
    final Props props1 = new Props();
    final Props props2 = new Props();

    final ImmutableFlowProps flowProps1 = ImmutableFlowProps.createFlowProps(props1);
    final ImmutableFlowProps flowProps2 = ImmutableFlowProps.createFlowProps(props2);

    Assert.assertNotSame("Both the FlowProps should refer to the same object because of inbuilt "
        + "interning.", flowProps1, flowProps2);
  }
}
