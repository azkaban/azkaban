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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import org.apache.log4j.Logger;


/**
 * Hashmap implementation of a hierarchical properties with helpful converter functions and
 * Exception throwing. This class is not threadsafe.
 */
public class Props {

  private final Map<String, String> _current;
  private Props _parent;
  private String source = null;

  /**
   * Constructor for empty props with empty parent.
   */
  public Props() {
    this(null);
  }

  /**
   * Constructor for empty Props with parent override.
   */
  public Props(final Props parent) {
    this._current = new HashMap<>();
    this._parent = parent;
  }

  /**
   * Load props from a file.
   */
  public Props(final Props parent, final String filepath) throws IOException {
    this(parent, new File(filepath));
  }

  /**
   * Load props from a file.
   */
  public Props(final Props parent, final File file) throws IOException {
    this(parent);

    if (file.exists()) {
      setSource(file.getPath());

      final InputStream input = new BufferedInputStream(new FileInputStream(file));
      try {
        loadFrom(input);
      } catch (final IOException e) {
        throw e;
      } finally {
        input.close();
      }
    }
  }

  /**
   * Create props from property input streams
   */
  public Props(final Props parent, final InputStream inputStream) throws IOException {
    this(parent);
    loadFrom(inputStream);
  }

  /**
   * Create properties from maps of properties
   */
  public Props(final Props parent, final Map<String, String>... props) {
    this(parent);
    for (int i = props.length - 1; i >= 0; i--) {
      this.putAll(props[i]);
    }
  }

  /**
   * Create properties from Properties objects
   */
  public Props(final Props parent, final Properties... properties) {
    this(parent);
    for (int i = properties.length - 1; i >= 0; i--) {
      this.put(properties[i]);
    }
  }

  /**
   * Create a Props object with the contents set to that of props.
   */
  public Props(final Props parent, final Props props) {
    this(parent);
    if (props != null) {
      putAll(props);
    }
  }

  /**
   * Create a Props with a null parent from a list of key value pairing. i.e. [key1, value1, key2,
   * value2 ...]
   */
  public static Props of(final String... args) {
    return of((Props) null, args);
  }

  /**
   * Create a Props from a list of key value pairing. i.e. [key1, value1, key2, value2 ...]
   */
  public static Props of(final Props parent, final String... args) {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Must have an equal number of keys and values.");
    }

    final Map<String, String> vals = new HashMap<>(args.length / 2);

    for (int i = 0; i < args.length; i += 2) {
      vals.put(args[i], args[i + 1]);
    }
    return new Props(parent, vals);
  }

  /**
   * Clones the Props p object and all of its parents.
   */
  public static Props clone(final Props p) {
    return copyNext(p);
  }

  /**
   * Recursive Clone function of Props
   *
   * @param source the source Props object
   * @return the cloned Props object
   */
  private static Props copyNext(final Props source) {
    Props priorNodeCopy = null;
    if (source.getParent() != null) {
      priorNodeCopy = copyNext(source.getParent());
    }
    final Props dest = new Props(priorNodeCopy);
    for (final String key : source.localKeySet()) {
      dest.put(key, source.get(key));
    }

    return dest;
  }

  /**
   * Create a new Props instance
   *
   * @param parent parent props
   * @param current current props
   * @param source source value
   * @return new Prop Instance
   */
  public static Props getInstance(Props parent, Props current, String source) {
    Props props = new Props(parent, current);
    props.setSource(source);
    return props;
  }

  /**
   * load this Prop Object from a @Properties formatted InputStream
   *
   * @param inputStream inputStream for loading Properties Object
   * @throws IOException read exception
   */
  private void loadFrom(final InputStream inputStream) throws IOException {
    final Properties properties = new Properties();
    properties.load(inputStream);
    this.put(properties);
  }

  /**
   * Get the Root Props Object
   *
   * @return the root Props Object or this Props itself
   */
  public Props getEarliestAncestor() {
    if (this._parent == null) {
      return this;
    }

    return this._parent.getEarliestAncestor();
  }

  /**
   * Set the Props Object as the root of this Props Object
   *
   * @param parent the earliest ancestor Props Object
   */
  public void setEarliestAncestor(final Props parent) {
    final Props props = getEarliestAncestor();
    props.setParent(parent);
  }

  /**
   * Clear the current Props, but leaves the parent untouched.
   */
  public void clearLocal() {
    this._current.clear();
  }

  /**
   * Check key in current Props then search in parent
   */
  public boolean containsKey(final Object k) {
    return this._current.containsKey(k)
        || (this._parent != null && this._parent.containsKey(k));
  }

  /**
   * Check value in current Props then search in parent
   */
  public boolean containsValue(final Object value) {
    return this._current.containsValue(value)
        || (this._parent != null && this._parent.containsValue(value));
  }

  /**
   * Return value if available in current Props otherwise return from parent
   */
  public String get(final Object key) {
    if (this._current.containsKey(key)) {
      return this._current.get(key);
    } else if (this._parent != null) {
      return this._parent.get(key);
    } else {
      return null;
    }
  }

  /**
   * Get the key set from the current Props
   */
  public Set<String> localKeySet() {
    return this._current.keySet();
  }

  /**
   * Get parent Props
   */
  public Props getParent() {
    return this._parent;
  }

  public void setParent(final Props prop) {
    this._parent = prop;
  }

  /**
   * Put the given string value for the string key. This method performs any variable substitution
   * in the value replacing any occurance of ${name} with the value of get("name").
   *
   * @param key The key to put the value to
   * @param value The value to do substitution on and store
   * @throws IllegalArgumentException If the variable given for substitution is not a valid key in
   * this Props.
   */
  public String put(final String key, final String value) {
    return this._current.put(key, value);
  }

  /**
   * Put the given Properties into the Props. This method performs any variable substitution in the
   * value replacing any occurrence of ${name} with the value of get("name"). get() is called first
   * on the Props and next on the Properties object.
   *
   * @param properties The properties to put
   * @throws IllegalArgumentException If the variable given for substitution is not a valid key in
   * this Props.
   */
  public void put(final Properties properties) {
    for (final String propName : properties.stringPropertyNames()) {
      this._current.put(propName, properties.getProperty(propName));
    }
  }

  /**
   * Put integer
   */
  public String put(final String key, final Integer value) {
    return this._current.put(key, value.toString());
  }

  /**
   * Put Long. Stores as String.
   */
  public String put(final String key, final Long value) {
    return this._current.put(key, value.toString());
  }

  /**
   * Put Double. Stores as String.
   */
  public String put(final String key, final Double value) {
    return this._current.put(key, value.toString());
  }

  /**
   * Put everything in the map into the props.
   */
  public void putAll(final Map<? extends String, ? extends String> m) {
    if (m == null) {
      return;
    }

    for (final Map.Entry<? extends String, ? extends String> entry : m.entrySet()) {
      this.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Put all properties in the props into the current props. Will handle null p.
   */
  public void putAll(final Props p) {
    if (p == null) {
      return;
    }

    for (final String key : p.getKeySet()) {
      this.put(key, p.get(key));
    }
  }

  /**
   * Puts only the local props from p into the current properties
   */
  public void putLocal(final Props p) {
    for (final String key : p.localKeySet()) {
      this.put(key, p.get(key));
    }
  }

  /**
   * Remove only the local value of key s, and not the parents.
   */
  public String removeLocal(final Object s) {
    return this._current.remove(s);
  }

  /**
   * The number of unique keys defined by this Props and all parent Props
   */
  public int size() {
    return getKeySet().size();
  }

  /**
   * The number of unique keys defined by this Props (keys defined only in parent Props are not
   * counted)
   */
  public int localSize() {
    return this._current.size();
  }

  /**
   * Attempts to return the Class that corresponds to the Props value. If the class doesn't exit, an
   * IllegalArgumentException will be thrown.
   */
  public Class<?> getClass(final String key) {
    try {
      if (containsKey(key)) {
        return Class.forName(get(key));
      } else {
        throw new UndefinedPropertyException("Missing required property '"
            + key + "'");
      }
    } catch (final ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public Class<?> getClass(final String key, final boolean initialize, final ClassLoader cl) {
    try {
      if (containsKey(key)) {
        return Class.forName(get(key), initialize, cl);
      } else {
        throw new UndefinedPropertyException("Missing required property '"
            + key + "'");
      }
    } catch (final ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Gets the class from the Props. If it doesn't exist, it will return the defaultClass
   */
  public Class<?> getClass(final String key, final Class<?> defaultClass) {
    if (containsKey(key)) {
      return getClass(key);
    } else {
      return defaultClass;
    }
  }

  /**
   * Gets the string from the Props. If it doesn't exist, it will return the defaultValue
   */
  public String getString(final String key, final String defaultValue) {
    if (containsKey(key)) {
      return get(key);
    } else {
      return defaultValue;
    }
  }

  /**
   * Gets the string from the Props. If it doesn't exist, throw and UndefinedPropertiesException
   */
  public String getString(final String key) {
    if (containsKey(key)) {
      return get(key);
    } else {
      throw new UndefinedPropertyException("Missing required property '" + key
          + "'");
    }
  }

  /**
   * Returns a list of strings with the comma as the separator of the value
   */
  public List<String> getStringList(final String key) {
    return getStringList(key, "\\s*,\\s*");
  }

  /**
   * Returns a list of clusters with the comma as the separator of the value
   * e.g., for input string: "thrift://hcat1:port,thrift://hcat2:port;thrift://hcat3:port,thrift://hcat4:port;"
   * we will get ["thrift://hcat1:port,thrift://hcat2:port", "thrift://hcat3:port,thrift://hcat4:port"]
   * as output
   */
  public List<String> getStringListFromCluster(final String key) {
    final List<String> curlist = getStringList(key, "\\s*;\\s*");
    // remove empty elements in the array
    for (final Iterator<String> iter = curlist.listIterator(); iter.hasNext(); ) {
      final String a = iter.next();
      if (a.length() == 0) {
        iter.remove();
      }
    }
    return curlist;
  }

  /**
   * Returns a list of strings with the sep as the separator of the value
   */
  public List<String> getStringList(final String key, final String sep) {
    String val = get(key);
    if (val == null || (val = val.trim()).length() == 0) {
      return Collections.emptyList();
    }
    return Arrays.asList(val.split(sep));
  }

  /**
   * Returns a list of strings with the comma as the separator of the value. If the value is null,
   * it'll return the defaultValue.
   */
  public List<String> getStringList(final String key, final List<String> defaultValue) {
    if (containsKey(key)) {
      return getStringList(key);
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns a list of strings with the sep as the separator of the value. If the value is null,
   * it'll return the defaultValue.
   */
  public List<String> getStringList(final String key, final List<String> defaultValue,
      final String sep) {
    if (containsKey(key)) {
      return getStringList(key, sep);
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns true if the value equals "true". If the value is null, then the default value is
   * returned.
   */
  public boolean getBoolean(final String key, final boolean defaultValue) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key).trim());
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns true if the value equals "true". If the value is null, then an
   * UndefinedPropertyException is thrown.
   */
  public boolean getBoolean(final String key) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key));
    } else {
      throw new UndefinedPropertyException("Missing required property '" + key
          + "'");
    }
  }

  /**
   * Returns the long representation of the value. If the value is null, then the default value is
   * returned. If the value isn't a long, then a parse exception will be thrown.
   */
  public long getLong(final String name, final long defaultValue) {
    if (containsKey(name)) {
      return Long.parseLong(get(name));
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns the long representation of the value. If the value is null, then a
   * UndefinedPropertyException will be thrown. If the value isn't a long, then a parse exception
   * will be thrown.
   */
  //todo burgerkingeater: it might be better to return null instead of throwing exception to
  // avoid repetitive exception handling
  public long getLong(final String name) {
    if (containsKey(name)) {
      return Long.parseLong(get(name));
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  /**
   * Returns the int representation of the value. If the value is null, then the default value is
   * returned. If the value isn't a int, then a parse exception will be thrown.
   */
  public int getInt(final String name, final int defaultValue) {
    if (containsKey(name)) {
      return Integer.parseInt(get(name).trim());
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns the int representation of the value. If the value is null, then a
   * UndefinedPropertyException will be thrown. If the value isn't a int, then a parse exception
   * will be thrown.
   */
  public int getInt(final String name) {
    if (containsKey(name)) {
      return Integer.parseInt(get(name).trim());
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  /**
   * Returns the double representation of the value. If the value is null, then the default value is
   * returned. If the value isn't a double, then a parse exception will be thrown.
   */
  public double getDouble(final String name, final double defaultValue) {
    if (containsKey(name)) {
      return Double.parseDouble(get(name).trim());
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns the double representation of the value. If the value is null, then a
   * UndefinedPropertyException will be thrown. If the value isn't a double, then a parse exception
   * will be thrown.
   */
  public double getDouble(final String name) {
    if (containsKey(name)) {
      return Double.parseDouble(get(name).trim());
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  /**
   * Returns the uri representation of the value. If the value is null, then an
   * UndefinedPropertyException will be thrown. If the value isn't a uri, then an
   * IllegalArgumentException will be thrown.
   *
   * If addTrailingSlash is true and the value isn't null, a trailing forward slash will be added
   * to the URI.
   */
  public URI getUri(final String name) { return getUri(name, false); }
  public URI getUri(final String name, final Boolean addTrailingSlash) {
    if (containsKey(name)) {
      try {
        String rawValue = get(name);
        if (rawValue == null) return null;

        String finalValue = !addTrailingSlash || rawValue.endsWith("/") ? rawValue : rawValue + "/";
        return new URI(finalValue);
      } catch (final URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  /**
   * Returns the double representation of the value. If the value is null, then the default value is
   * returned. If the value isn't a uri, then a IllegalArgumentException will be thrown.
   *
   * If addTrailingSlash is true and the value isn't null, a trailing forward slash will be added
   * to the URI.
   */
  public URI getUri(final String name, final URI defaultValue) { return getUri(name, defaultValue, false); }
  public URI getUri(final String name, final URI defaultValue, final Boolean addTrailingSlash) {
    if (containsKey(name)) {
      return getUri(name, addTrailingSlash);
    } else {
      return defaultValue;
    }
  }

  /**
   * Convert a URI-formatted string value to URI object
   */
  public URI getUri(final String name, final String defaultValue) {
    try {
      return getUri(name, new URI(defaultValue));
    } catch (final URISyntaxException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  /**
   * Store only those properties defined at this local level
   *
   * @param file The file to write to
   * @throws IOException If the file can't be found or there is an io error
   */
  public void storeLocal(final File file) throws IOException {
    final BufferedOutputStream out =
        new BufferedOutputStream(new FileOutputStream(file));
    try {
      storeLocal(out);
    } finally {
      out.close();
    }
  }

  /**
   * Returns a copy of only the local values of this props
   */
  public Props local() {
    return new Props(null, this._current);
  }

  /**
   * Store only those properties defined at this local level
   *
   * @param out The output stream to write to
   * @throws IOException If the file can't be found or there is an io error
   */
  public void storeLocal(final OutputStream out) throws IOException {
    final Properties p = new Properties();
    for (final String key : this._current.keySet()) {
      p.setProperty(key, get(key));
    }
    p.store(out, null);
  }

  /**
   * Returns a java.util.Properties file populated with the current Properties in here.
   * Note: if you want to import parent properties (e.g., database credentials), please use
   * toAllProperties
   */
  public Properties toProperties() {
    final Properties p = new Properties();
    for (final String key : this._current.keySet()) {
      p.setProperty(key, get(key));
    }

    return p;
  }

  /**
   * Returns a java.util.Properties file populated with both current and parent properties.
   */
  public Properties toAllProperties() {
    final Properties allProp = new Properties();

    // import parent properties
    if (this._parent != null) {
      allProp.putAll(this._parent.toProperties());
    }

    // import local properties
    allProp.putAll(toProperties());

    return allProp;
  }

  /**
   * Store all properties, those local and also those in parent props
   *
   * @param file The file to store to
   * @throws IOException If there is an error writing
   */
  public void storeFlattened(final File file) throws IOException {
    final BufferedOutputStream out =
        new BufferedOutputStream(new FileOutputStream(file));
    try {
      storeFlattened(out);
    } finally {
      out.close();
    }
  }

  /**
   * Store all properties, those local and also those in parent props
   *
   * @param out The stream to write to
   * @throws IOException If there is an error writing
   */
  public void storeFlattened(final OutputStream out) throws IOException {
    final Properties p = new Properties();
    for (Props curr = this; curr != null; curr = curr.getParent()) {
      for (final String key : curr.localKeySet()) {
        if (!p.containsKey(key)) {
          p.setProperty(key, get(key));
        }
      }
    }

    p.store(out, null);
  }

  /**
   * Returns a new constructed map of all the flattened properties, the item in the returned
   * map is sorted alphabetically by the key value.
   *
   * @Return a new constructed TreeMap (sorted map) of all properties (including parents'
   * properties)
   */
  public Map<String, String> getFlattened() {
    final TreeMap<String, String> returnVal = new TreeMap<>();
    returnVal.putAll(getMapByPrefix(""));
    return returnVal;
  }

  /**
   * Get a new de-duplicated map of all the flattened properties by given prefix. The prefix will
   * be removed in the return map's keySet.
   *
   * @param prefix the prefix string
   * @return a new constructed de-duplicated HashMap of all properties (including parents'
   * properties) with the give prefix
   */
  public Map<String, String> getMapByPrefix(final String prefix) {
    final Map<String, String> values = (this._parent == null)
        ? new HashMap<>()
        : this._parent.getMapByPrefix(prefix);

    // when there is a conflict, value from the child takes the priority.
    if (prefix == null) { // when prefix is null, return an empty map
      return values;
    }

    for (final String key : this.localKeySet()) {
      if (key != null && key.length() >= prefix.length()) {
        if (key.startsWith(prefix)) {
          values.put(key.substring(prefix.length()), get(key));
        }
      }
    }
    return values;
  }

  /**
   * Returns a set of all keys, including the parents
   */
  public Set<String> getKeySet() {
    final HashSet<String> keySet = new HashSet<>();

    keySet.addAll(localKeySet());

    if (this._parent != null) {
      keySet.addAll(this._parent.getKeySet());
    }

    return keySet;
  }

  /**
   * Logs the property in the given logger
   */
  public void logProperties(final Logger logger, final String comment) {
    logger.info(comment);

    for (final String key : getKeySet()) {
      logger.info("  key=" + key + " value=" + get(key));
    }
  }

  /**
   * override object's default equal function
   */
  @Override
  public boolean equals(final Object o) {
    if (o == this) {
      return true;
    } else if (o == null) {
      return false;
    } else if (o.getClass() != Props.class) {
      return false;
    }

    final Props p = (Props) o;
    return this._current.equals(p._current) && Utils.equals(this._parent, p._parent);
  }

  /**
   * override object's default hash code function
   */
  @Override
  public int hashCode() {
    int code = this._current.hashCode();
    if (this._parent != null) {
      code += this._parent.hashCode();
    }
    return code;
  }

  /**
   * override object's default toString function
   */
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder("{");
    for (final Map.Entry<String, String> entry : this._current.entrySet()) {
      builder.append(entry.getKey());
      builder.append(": ");
      builder.append(entry.getValue());
      builder.append(", ");
    }
    if (this._parent != null) {
      builder.append(" parent = ");
      builder.append(this._parent.toString());
    }
    builder.append("}");
    return builder.toString();
  }

  /**
   * Get Source information
   */
  public String getSource() {
    return this.source;
  }

  /**
   * Set Source information
   */
  public Props setSource(final String source) {
    this.source = source;
    return this;
  }
}
