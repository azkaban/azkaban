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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * Hashmap implementation of a hierarchitical properties with helpful converter
 * functions and Exception throwing. This class is not threadsafe.
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
   *
   * @param parent
   */
  public Props(Props parent) {
    this._current = new HashMap<String, String>();
    this._parent = parent;
  }

  /**
   * Load props from a file.
   *
   * @param parent
   * @param file
   * @throws IOException
   */
  public Props(Props parent, String filepath) throws IOException {
    this(parent, new File(filepath));
  }

  /**
   * Load props from a file.
   *
   * @param parent
   * @param file
   * @throws IOException
   */
  public Props(Props parent, File file) throws IOException {
    this(parent);
    setSource(file.getPath());

    InputStream input = new BufferedInputStream(new FileInputStream(file));
    try {
      loadFrom(input);
    } catch (IOException e) {
      throw e;
    } finally {
      input.close();
    }
  }

  /**
   * Create props from property input streams
   *
   * @param parent
   * @param inputStreams
   * @throws IOException
   */
  public Props(Props parent, InputStream inputStream) throws IOException {
    this(parent);
    loadFrom(inputStream);
  }

  /**
   *
   * @param inputStream
   * @throws IOException
   */
  private void loadFrom(InputStream inputStream) throws IOException {
    Properties properties = new Properties();
    properties.load(inputStream);
    this.put(properties);
  }

  /**
   * Create properties from maps of properties
   *
   * @param parent
   * @param props
   */
  public Props(Props parent, Map<String, String>... props) {
    this(parent);
    for (int i = props.length - 1; i >= 0; i--) {
      this.putAll(props[i]);
    }
  }

  /**
   * Create properties from Properties objects
   *
   * @param parent
   * @param properties
   */
  public Props(Props parent, Properties... properties) {
    this(parent);
    for (int i = properties.length - 1; i >= 0; i--) {
      this.put(properties[i]);
    }
  }

  /**
   * Create a Props object with the contents set to that of props.
   *
   * @param parent
   * @param props
   */
  public Props(Props parent, Props props) {
    this(parent);
    if (props != null) {
      putAll(props);
    }
  }

  public void setEarliestAncestor(Props parent) {
    Props props = getEarliestAncestor();
    props.setParent(parent);
  }

  public Props getEarliestAncestor() {
    if (_parent == null) {
      return this;
    }

    return _parent.getEarliestAncestor();
  }

  /**
   * Create a Props with a null parent from a list of key value pairing. i.e.
   * [key1, value1, key2, value2 ...]
   *
   * @param args
   * @return
   */
  public static Props of(String... args) {
    return of((Props) null, args);
  }

  /**
   * Create a Props from a list of key value pairing. i.e. [key1, value1, key2,
   * value2 ...]
   *
   * @param args
   * @return
   */
  @SuppressWarnings("unchecked")
  public static Props of(Props parent, String... args) {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(
          "Must have an equal number of keys and values.");
    }

    Map<String, String> vals = new HashMap<String, String>(args.length / 2);

    for (int i = 0; i < args.length; i += 2) {
      vals.put(args[i], args[i + 1]);
    }
    return new Props(parent, vals);
  }

  /**
   * Clear the current Props, but leaves the parent untouched.
   */
  public void clearLocal() {
    _current.clear();
  }

  /**
   * Check key in current Props then search in parent
   *
   * @param k
   * @return
   */
  public boolean containsKey(Object k) {
    return _current.containsKey(k)
        || (_parent != null && _parent.containsKey(k));
  }

  /**
   * Check value in current Props then search in parent
   *
   * @param value
   * @return
   */
  public boolean containsValue(Object value) {
    return _current.containsValue(value)
        || (_parent != null && _parent.containsValue(value));
  }

  /**
   * Return value if available in current Props otherwise return from parent
   *
   * @param key
   * @return
   */
  public String get(Object key) {
    if (_current.containsKey(key)) {
      return _current.get(key);
    } else if (_parent != null) {
      return _parent.get(key);
    } else {
      return null;
    }
  }

  /**
   * Get the key set from the current Props
   *
   * @return
   */
  public Set<String> localKeySet() {
    return _current.keySet();
  }

  /**
   * Get parent Props
   *
   * @return
   */
  public Props getParent() {
    return _parent;
  }

  /**
   * Put the given string value for the string key. This method performs any
   * variable substitution in the value replacing any occurance of ${name} with
   * the value of get("name").
   *
   * @param key The key to put the value to
   * @param value The value to do substitution on and store
   *
   * @throws IllegalArgumentException If the variable given for substitution is
   *           not a valid key in this Props.
   */
  public String put(String key, String value) {
    return _current.put(key, value);
  }

  /**
   * Put the given Properties into the Props. This method performs any variable
   * substitution in the value replacing any occurrence of ${name} with the
   * value of get("name"). get() is called first on the Props and next on the
   * Properties object.
   *
   * @param properties The properties to put
   *
   * @throws IllegalArgumentException If the variable given for substitution is
   *           not a valid key in this Props.
   */
  public void put(Properties properties) {
    for (String propName : properties.stringPropertyNames()) {
      _current.put(propName, properties.getProperty(propName));
    }
  }

  /**
   * Put integer
   *
   * @param key
   * @param value
   * @return
   */
  public String put(String key, Integer value) {
    return _current.put(key, value.toString());
  }

  /**
   * Put Long. Stores as String.
   *
   * @param key
   * @param value
   * @return
   */
  public String put(String key, Long value) {
    return _current.put(key, value.toString());
  }

  /**
   * Put Double. Stores as String.
   *
   * @param key
   * @param value
   * @return
   */
  public String put(String key, Double value) {
    return _current.put(key, value.toString());
  }

  /**
   * Put everything in the map into the props.
   *
   * @param m
   */
  public void putAll(Map<? extends String, ? extends String> m) {
    if (m == null) {
      return;
    }

    for (Map.Entry<? extends String, ? extends String> entry : m.entrySet()) {
      this.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Put all properties in the props into the current props. Will handle null p.
   *
   * @param p
   */
  public void putAll(Props p) {
    if (p == null) {
      return;
    }

    for (String key : p.getKeySet()) {
      this.put(key, p.get(key));
    }
  }

  /**
   * Puts only the local props from p into the current properties
   *
   * @param p
   */
  public void putLocal(Props p) {
    for (String key : p.localKeySet()) {
      this.put(key, p.get(key));
    }
  }

  /**
   * Remove only the local value of key s, and not the parents.
   *
   * @param s
   * @return
   */
  public String removeLocal(Object s) {
    return _current.remove(s);
  }

  /**
   * The number of unique keys defined by this Props and all parent Props
   */
  public int size() {
    return getKeySet().size();
  }

  /**
   * The number of unique keys defined by this Props (keys defined only in
   * parent Props are not counted)
   */
  public int localSize() {
    return _current.size();
  }

  /**
   * Attempts to return the Class that corresponds to the Props value. If the
   * class doesn't exit, an IllegalArgumentException will be thrown.
   *
   * @param key
   * @return
   */
  public Class<?> getClass(String key) {
    try {
      if (containsKey(key)) {
        return Class.forName(get(key));
      } else {
        throw new UndefinedPropertyException("Missing required property '"
            + key + "'");
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public Class<?> getClass(String key, boolean initialize, ClassLoader cl) {
    try {
      if (containsKey(key)) {
        return Class.forName(get(key), initialize, cl);
      } else {
        throw new UndefinedPropertyException("Missing required property '"
            + key + "'");
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Gets the class from the Props. If it doesn't exist, it will return the
   * defaultClass
   *
   * @param key
   * @param c
   * @return
   */
  public Class<?> getClass(String key, Class<?> defaultClass) {
    if (containsKey(key)) {
      return getClass(key);
    } else {
      return defaultClass;
    }
  }

  /**
   * Gets the string from the Props. If it doesn't exist, it will return the
   * defaultValue
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public String getString(String key, String defaultValue) {
    if (containsKey(key)) {
      return get(key);
    } else {
      return defaultValue;
    }
  }

  /**
   * Gets the string from the Props. If it doesn't exist, throw and
   * UndefinedPropertiesException
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public String getString(String key) {
    if (containsKey(key)) {
      return get(key);
    } else {
      throw new UndefinedPropertyException("Missing required property '" + key
          + "'");
    }
  }

  /**
   * Returns a list of strings with the comma as the separator of the value
   *
   * @param key
   * @return
   */
  public List<String> getStringList(String key) {
    return getStringList(key, "\\s*,\\s*");
  }

  /**
   * Returns a list of strings with the sep as the separator of the value
   *
   * @param key
   * @param sep
   * @return
   */
  public List<String> getStringList(String key, String sep) {
    String val = get(key);
    if (val == null || val.trim().length() == 0) {
      return Collections.emptyList();
    }

    if (containsKey(key)) {
      return Arrays.asList(val.split(sep));
    } else {
      throw new UndefinedPropertyException("Missing required property '" + key
          + "'");
    }
  }

  /**
   * Returns a list of strings with the comma as the separator of the value. If
   * the value is null, it'll return the defaultValue.
   *
   * @param key
   * @return
   */
  public List<String> getStringList(String key, List<String> defaultValue) {
    if (containsKey(key)) {
      return getStringList(key);
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns a list of strings with the sep as the separator of the value. If
   * the value is null, it'll return the defaultValue.
   *
   * @param key
   * @return
   */
  public List<String> getStringList(String key, List<String> defaultValue,
      String sep) {
    if (containsKey(key)) {
      return getStringList(key, sep);
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns true if the value equals "true". If the value is null, then the
   * default value is returned.
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    if (containsKey(key)) {
      return "true".equalsIgnoreCase(get(key).trim());
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns true if the value equals "true". If the value is null, then an
   * UndefinedPropertyException is thrown.
   *
   * @param key
   * @return
   */
  public boolean getBoolean(String key) {
    if (containsKey(key))
      return "true".equalsIgnoreCase(get(key));
    else
      throw new UndefinedPropertyException("Missing required property '" + key
          + "'");
  }

  /**
   * Returns the long representation of the value. If the value is null, then
   * the default value is returned. If the value isn't a long, then a parse
   * exception will be thrown.
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public long getLong(String name, long defaultValue) {
    if (containsKey(name)) {
      return Long.parseLong(get(name));
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns the long representation of the value. If the value is null, then a
   * UndefinedPropertyException will be thrown. If the value isn't a long, then
   * a parse exception will be thrown.
   *
   * @param key
   * @return
   */
  public long getLong(String name) {
    if (containsKey(name)) {
      return Long.parseLong(get(name));
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  /**
   * Returns the int representation of the value. If the value is null, then the
   * default value is returned. If the value isn't a int, then a parse exception
   * will be thrown.
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public int getInt(String name, int defaultValue) {
    if (containsKey(name)) {
      return Integer.parseInt(get(name).trim());
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns the int representation of the value. If the value is null, then a
   * UndefinedPropertyException will be thrown. If the value isn't a int, then a
   * parse exception will be thrown.
   *
   * @param key
   * @return
   */
  public int getInt(String name) {
    if (containsKey(name)) {
      return Integer.parseInt(get(name).trim());
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  /**
   * Returns the double representation of the value. If the value is null, then
   * the default value is returned. If the value isn't a double, then a parse
   * exception will be thrown.
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public double getDouble(String name, double defaultValue) {
    if (containsKey(name)) {
      return Double.parseDouble(get(name).trim());
    } else {
      return defaultValue;
    }
  }

  /**
   * Returns the double representation of the value. If the value is null, then
   * a UndefinedPropertyException will be thrown. If the value isn't a double,
   * then a parse exception will be thrown.
   *
   * @param key
   * @return
   */
  public double getDouble(String name) {
    if (containsKey(name)) {
      return Double.parseDouble(get(name).trim());
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  /**
   * Returns the uri representation of the value. If the value is null, then the
   * default value is returned. If the value isn't a uri, then a
   * IllegalArgumentException will be thrown.
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public URI getUri(String name) {
    if (containsKey(name)) {
      try {
        return new URI(get(name));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    } else {
      throw new UndefinedPropertyException("Missing required property '" + name
          + "'");
    }
  }

  /**
   * Returns the double representation of the value. If the value is null, then
   * the default value is returned. If the value isn't a uri, then a
   * IllegalArgumentException will be thrown.
   *
   * @param key
   * @param defaultValue
   * @return
   */
  public URI getUri(String name, URI defaultValue) {
    if (containsKey(name)) {
      return getUri(name);
    } else {
      return defaultValue;
    }
  }

  public URI getUri(String name, String defaultValue) {
    try {
      return getUri(name, new URI(defaultValue));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  /**
   * Store only those properties defined at this local level
   *
   * @param file The file to write to
   * @throws IOException If the file can't be found or there is an io error
   */
  public void storeLocal(File file) throws IOException {
    BufferedOutputStream out =
        new BufferedOutputStream(new FileOutputStream(file));
    try {
      storeLocal(out);
    } finally {
      out.close();
    }
  }

  /**
   * Returns a copy of only the local values of this props
   *
   * @return
   */
  @SuppressWarnings("unchecked")
  public Props local() {
    return new Props(null, _current);
  }

  /**
   * Store only those properties defined at this local level
   *
   * @param out The output stream to write to
   * @throws IOException If the file can't be found or there is an io error
   */
  public void storeLocal(OutputStream out) throws IOException {
    Properties p = new Properties();
    for (String key : _current.keySet()) {
      p.setProperty(key, get(key));
    }
    p.store(out, null);
  }

  /**
   * Returns a java.util.Properties file populated with the stuff in here.
   *
   * @return
   */
  public Properties toProperties() {
    Properties p = new Properties();
    for (String key : _current.keySet()) {
      p.setProperty(key, get(key));
    }

    return p;
  }

  /**
   * Store all properties, those local and also those in parent props
   *
   * @param file The file to store to
   * @throws IOException If there is an error writing
   */
  public void storeFlattened(File file) throws IOException {
    BufferedOutputStream out =
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
  public void storeFlattened(OutputStream out) throws IOException {
    Properties p = new Properties();
    for (Props curr = this; curr != null; curr = curr.getParent()) {
      for (String key : curr.localKeySet()) {
        if (!p.containsKey(key)) {
          p.setProperty(key, get(key));
        }
      }
    }

    p.store(out, null);
  }

  /**
   * Returns a map of all the flattened properties, the item in the returned map is sorted alphabetically
   * by the key value.
   *
   *
   * @Return
   */
  public Map<String,String> getFlattened(){
    TreeMap<String,String> returnVal = new TreeMap<String,String>(); 
    returnVal.putAll(getMapByPrefix(""));
    return returnVal; 
  }

  /**
   * Get a map of all properties by string prefix
   *
   * @param prefix The string prefix
   */
  public Map<String, String> getMapByPrefix(String prefix) {
    Map<String, String> values = _parent == null ? new HashMap<String, String>():
                                                   _parent.getMapByPrefix(prefix);

    // when there is a conflict, value from the child takes the priority.
    for (String key : this.localKeySet()) {
      if (key.startsWith(prefix)) {
        values.put(key.substring(prefix.length()), get(key));
      }
    }
    return values;
  }

  /**
   * Returns a set of all keys, including the parents
   *
   * @return
   */
  public Set<String> getKeySet() {
    HashSet<String> keySet = new HashSet<String>();

    keySet.addAll(localKeySet());

    if (_parent != null) {
      keySet.addAll(_parent.getKeySet());
    }

    return keySet;
  }

  /**
   * Logs the property in the given logger
   *
   * @param logger
   * @param comment
   */
  public void logProperties(Logger logger, String comment) {
    logger.info(comment);

    for (String key : getKeySet()) {
      logger.info("  key=" + key + " value=" + get(key));
    }
  }

  /**
   * Clones the Props p object and all of its parents.
   *
   * @param p
   * @return
   */
  public static Props clone(Props p) {
    return copyNext(p);
  }

  /**
   *
   * @param source
   * @return
   */
  private static Props copyNext(Props source) {
    Props priorNodeCopy = null;
    if (source.getParent() != null) {
      priorNodeCopy = copyNext(source.getParent());
    }
    Props dest = new Props(priorNodeCopy);
    for (String key : source.localKeySet()) {
      dest.put(key, source.get(key));
    }

    return dest;
  }

  /**
     */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } else if (o == null) {
      return false;
    } else if (o.getClass() != Props.class) {
      return false;
    }

    Props p = (Props) o;
    return _current.equals(p._current) && Utils.equals(this._parent, p._parent);
  }

  /**
   * Returns true if the properties are equivalent, regardless of the hierarchy.
   *
   * @param p
   * @return
   */
  public boolean equalsProps(Props p) {
    if (p == null) {
      return false;
    }

    final Set<String> myKeySet = getKeySet();
    for (String s : myKeySet) {
      if (!get(s).equals(p.get(s))) {
        return false;
      }
    }

    return myKeySet.size() == p.getKeySet().size();
  }

  /**
     *
     */
  @Override
  public int hashCode() {
    int code = this._current.hashCode();
    if (_parent != null)
      code += _parent.hashCode();
    return code;
  }

  /**
     *
     */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    for (Map.Entry<String, String> entry : this._current.entrySet()) {
      builder.append(entry.getKey());
      builder.append(": ");
      builder.append(entry.getValue());
      builder.append(", ");
    }
    if (_parent != null) {
      builder.append(" parent = ");
      builder.append(_parent.toString());
    }
    builder.append("}");
    return builder.toString();
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public void setParent(Props prop) {
    this._parent = prop;
  }
}
