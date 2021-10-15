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

package azkaban.flow;

import azkaban.utils.Props;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.util.HashMap;
import java.util.Map;


public final class ImmutableFlowProps {

  private String parentSource;
  private String propSource;
  private Props props = null;

  private static final Interner<ImmutableFlowProps> interner = Interners.newWeakInterner();

  // Q. Why is it important for this class to be immutable?
  // ====================================================
  // Ans. We want to support internability for the objects of this class, which requires the class
  // to be immutable. Furthermore, this class is 'final' to disallow accidental creation of a
  //mutable subclass.
  //
  // Q. What is 'interning'?
  //
  // Ans. The concept of interning makes two references that would otherwise have pointed to
  //distinct objects, ending up referencing the same object, if the contents are equal.
  //
  // Q. What all is needed to achieve internability ?
  //
  // Ans.
  // Following are needed.
  // 1. Immutability: Two seemingly distinct references could be pointing to the same object.
  // Changing the contents of any one, will end up changing the contents for others as well.
  // Hence, once an object has been interned , it should not change.
  // 2. Equals - Define what it means to be equals. This also end up defining when will
  // interning trigger reuse i.e. The reference to an earlier object will be used.
  // 3. hashcode - Define a hashcode that is in-line with equals(). equals() will be checked only
  // for object whose hashcode is the same.
  //
  // Q. How does one specify the circumstances in which an object is to be interned?
  //
  // Ans. Interning a new Object, with the existing instances. It first checks the hashcode and then
  // check for equality (equals). If both specify, the new object being equivalent - the earlier
  // equivalent object is used and the new one is freed.

  public static ImmutableFlowProps createFlowProps(final String parentSource, final String propSource) {
    ImmutableFlowProps candidate = new ImmutableFlowProps(parentSource, propSource);
    return interner.intern(candidate);
  }

  public static ImmutableFlowProps createFlowProps(final Props props) {
    ImmutableFlowProps candidate = new ImmutableFlowProps(props);
    return interner.intern(candidate);
  }

  public ImmutableFlowProps(final String parentSource, final String propSource) {
    /**
     * Use String interning so that just 1 copy of the string value exists in String Constant Pool
     * and the value is reused. Azkaban Heap dump analysis indicated a  high percentage of heap
     * usage is coming from duplicate strings of FlowProps fields.
     *
     * Using intern() eliminates all the duplicate values, thereby significantly reducing heap
     * memory usage.
     */
    if(parentSource != null) {
      this.parentSource = parentSource.intern();
    }
    if (propSource != null) {
      this.propSource = propSource.intern();
    }
  }

  private ImmutableFlowProps(final Props props) {
    this.setProps(props);
  }

  static ImmutableFlowProps fromObject(final Object obj) {
    final Map<String, Object> flowMap = (Map<String, Object>) obj;
    final String source = (String) flowMap.get("source");
    final String parentSource = (String) flowMap.get("inherits");

    final ImmutableFlowProps immutableFlowProps = createFlowProps(parentSource, source);
    return immutableFlowProps;
  }

  public Props getProps() {
    return this.props;
  }

  /**
   * Sets Props as a reference for this class. In addition, it also copies source(s) of the
   * specified props and its parent.
   * This method should only be called from the constructor, because we want this class to be
   * immutable.
   * @param props The reference of props this class stores.
   */
  private void setProps(final Props props) {
    this.props = props;
    String parentSource = props.getParent() == null ? null : props.getParent().getSource();

    setParentSource(parentSource);
    setPropSource(props.getSource());
  }

  private void setParentSource(final String parentSource) {
    this.parentSource = (parentSource != null) ? parentSource.intern() : null;
  }

  private void setPropSource(final String propSource) {
    this.propSource = (propSource != null) ? propSource.intern() : null;
  }

  public String getSource() {
    return this.propSource;
  }

  public String getInheritedSource() {
    return this.parentSource;
  }

  public Object toObject() {
    final HashMap<String, Object> obj = new HashMap<>();
    obj.put("source", this.propSource);
    if (this.parentSource != null) {
      obj.put("inherits", this.parentSource);
    }
    return obj;
  }

  private static boolean equalRef(final Object ref1, final Object ref2) {
      if (ref1 == ref2) {
        return true;
      }

      if ((ref1 == null) || (ref2 == null)) {
        return false;
      }

      return false;
  }



  @Override
  /**
   * Tells whether the two objects can be considered equivalent.
   *
   * This equivalence will not be breakable. Once the two objects are equivalent, they are
   * guaranteed to always be equivalent.
   */
  public boolean equals(Object obj) {
    /*
    Since ImmutableFlowProps will be interned based on equivalence, this equivalence should not
    be breakable. Breaking it will cause overwrittes to seemingly unrelated references.

    In other words, if something can be edited later, then consider the objects to be
    non-equivalent, even if there contents are the same.
   */
    if (obj == null) {
      return false;
    }

    if (obj.getClass() != this.getClass()) {
      return false;
    }

    final ImmutableFlowProps other = (ImmutableFlowProps) obj;

    if (!equalRef(this.parentSource, other.parentSource) &&
        !this.parentSource.equals(other.parentSource)) {
      return false;
    }

    if (!equalRef(this.propSource, other.propSource) &&
        !this.propSource.equals(other.propSource)) {
      return false;
    }

    // Even though the contents of two props might be the same, the two will be considered to
    // different if their objects are different. This is because Props are mutable, and future
    // equivalence may be broken.
    if (!equalRef(this.props, other.props)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    // For reference:
    // https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/builder/HashCodeBuilder.html

    // The hashCodeBuilder_17_37 maintains a state, hence it has to be created afresh.
    // Otherwise, the hash-code will be different for the same inputs.
    HashCodeBuilder hashCodeBuilder_17_37 = new HashCodeBuilder(17, 37);
    return hashCodeBuilder_17_37
        .append(parentSource)
        .append(propSource)
        .append(props)
        .toHashCode();
  }

  public void Print() {
    System.out.println("this = " + System.identityHashCode(this));
    System.out.println("parentSource = " + System.identityHashCode(parentSource));
    System.out.println("propSource = " + System.identityHashCode(propSource));
    System.out.println("props = " + System.identityHashCode(props));
  }
}
