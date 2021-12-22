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
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import java.util.HashMap;
import java.util.Map;


/**
 * A wrapper over Props.
 *
 * ImmutableFlowProps stores a reference to props, along with the name of the prop's source, and
 * the name of prop's parent's source.
 */
public final class ImmutableFlowProps {

  private final String parentSource;
  private final String propSource;
  private final Props props;

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

  /**
   * Factory for building ImmutableFlowProps, and initialize it with the names of prop's source
   * and parent source.
   *
   * The factory returns a reference of deduped object. Instead of returning a duplicate, it
   * returns the reference of the pre-existing object. De-dup process checks for the name of prop's
   * source and parent source to be the same. It also checks for reference to prop being the same.
   * Two distinct references for the props, even if their contents are the sane, will be treated
   * as non-equivalent.
   *
   * @param parentSource Name of the prop's parents' source
   * @param propSource Name of the prop's source
   * @return A de-duplicated reference to ImmutableFlowProps
   */
  public static ImmutableFlowProps createFlowProps(final String parentSource, final String propSource) {
    ImmutableFlowProps candidate = new ImmutableFlowProps(parentSource, propSource);
    return interner.intern(candidate);
  }

  /**
   * Factory for building ImmutableFlowProps, and initialize it with the props object.
   * The the names of prop's source and parent source, are retrieved from the props object
   * itself.
   *
   * The factory returns a reference of de-duped object. Instead of returning a duplicate, it
   * returns the reference of the pre-existing object. De-dup process checks for the name of prop's
   * source and parent source to be the same. It also checks for reference to prop being the same.
   * Two distinct references for the props, even if their contents are the sane, will be treated
   * as non-equivalent.
   *
   * @param props The reference of props this class stores.
   * @return A de-duplicated reference to ImmutableFlowProps
   */
  public static ImmutableFlowProps createFlowProps(final Props props) {
    ImmutableFlowProps candidate = new ImmutableFlowProps(props);
    return interner.intern(candidate);
  }

  private ImmutableFlowProps(final String parentSource, final String propSource) {
    /**
     * Use String interning so that just 1 copy of the string value exists in String Constant Pool
     * and the value is reused. Azkaban Heap dump analysis indicated a  high percentage of heap
     * usage is coming from duplicate strings of FlowProps fields.
     *
     * Using intern() eliminates all the duplicate values, thereby significantly reducing heap
     * memory usage.
     */

    this.parentSource = (parentSource != null) ? parentSource.intern() : null;
    this.propSource = (propSource != null) ? propSource.intern() : null;
    this.props = null;
  }

  /**
   * Sets Props as a reference for this class. In addition, it also copies source(s) of the
   * specified props and its parent.
   *
   * @param props The reference of props this class stores.
   */
  private ImmutableFlowProps(final Props props) {
    this.props = props;
    String parentSource = props.getParent() == null ? null : props.getParent().getSource();
    String propSource = props.getSource();

    this.parentSource = (parentSource != null) ? parentSource.intern() : null;
    this.propSource = (propSource != null) ? propSource.intern() : null;
  }

  /**
   * Deserealizes an Object into ImmutableFlowProps.
   *
   * source and parentSource fields are the only ones that are part of
   * serialization/deserialization.
   *
   * @param obj The objects from which ImmutableFlowProps is deserialized.
   * @return ImmutableFlowProps that's created upon deserialization.
   * @see #toObject()
   */
  static ImmutableFlowProps fromObject(final Object obj) {
    final Map<String, Object> flowMap = (Map<String, Object>) obj;
    final String source = (String) flowMap.get("source");
    final String parentSource = (String) flowMap.get("inherits");

    final ImmutableFlowProps immutableFlowProps = createFlowProps(parentSource, source);
    return immutableFlowProps;
  }

  /**
   * Returns the props
   *
   * @return Prop Props of this object.
   */
  public Props getProps() {
    return this.props;
  }

  /**
   * Returns the name of the source.
   *
   * @return the name of the prop's source
   */
  public String getSource() {
    return this.propSource;
  }

  /**
   * Returns the name of the prop's parents' source.
   *
   * @return The name of the prop's parents' source.
   */
  public String getInheritedSource() {
    return this.parentSource;
  }

  /**
   * Serializes the ImmutableFlowProps into an Object.
   *
   * The 'propSource' and 'parentSource' are part of the serialization. The props is left out.
   *
   * @return The serialized Object.
   * @see #fromObject(Object)
   */
  public Object toObject() {
    final HashMap<String, Object> obj = new HashMap<>();
    obj.put("source", this.propSource);
    if (this.parentSource != null) {
      obj.put("inherits", this.parentSource);
    }
    return obj;
  }

  // Tells whether the two references pointing to the same object.
  private static boolean equalRef(final Object ref1, final Object ref2) {
      if (ref1 == ref2) {
        return true;
      }

      if ((ref1 == null) || (ref2 == null)) {
        return false;
      }

      return false;
  }

  /**
   * Tells whether the two objects can be considered equivalent.
   *
   * The class and this function are designed so that the equivalence will not be breakable. Once
   * the two objects are equivalent, they are guaranteed to always be equivalent.
   *
   * @param obj The reference object to compare with.
   * @see #hashCode()
   */
  @Override
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

    // Even though the contents of two props might be the same, the two will be considered to be
    // different if their objects are different. This is because Props are mutable, and future
    // equivalence may be broken.
    if (!equalRef(this.props, other.props)) {
      return false;
    }

    return true;
  }

  /**
   * Returns a hash code for this object.
   * @return Hash of this object.
   */
  @Override
  public int hashCode() {
    // This method gets used by interning.
    // Interning, first uses hashcode. If hashcode is same, it then uses equals/to deterine whether
    //an object can be interned.

    // The hashCodeBuilder_17_37 maintains a state, hence it has to be created afresh.
    // Otherwise, the hash-code will be different for the same inputs.
    // For reference:
    // https://commons.apache.org/proper/commons-lang/apidocs/org/apache/commons/lang3/builder/HashCodeBuilder.html
    HashCodeBuilder hashCodeBuilder_17_37 = new HashCodeBuilder(17, 37);
    return hashCodeBuilder_17_37
        .append(parentSource)
        .append(propSource)
        .append(props)
        .toHashCode();
  }

  /**
   * Prints identity hash codes for the fields in this object.
   *
   * This method very handy to use this with unit-test and understand  the interning.
   */
  public void print() {
    System.out.println("this = " + System.identityHashCode(this));
    System.out.println("parentSource = " + System.identityHashCode(parentSource));
    System.out.println("propSource = " + System.identityHashCode(propSource));
    System.out.println("props = " + System.identityHashCode(props));
  }
}
