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

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility Functions related to Prop Operations
 */
public class PropsUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PropsUtils.class);
  private static final Pattern VARIABLE_REPLACEMENT_PATTERN = Pattern
      .compile("\\$\\{([a-zA-Z_.0-9]+)\\}");

  /**
   * Private constructor.
   */
  private PropsUtils() {
  }


  /**
   * Load job schedules from the given directories
   *
   * @param dir The directory to look in
   * @param suffixes File suffixes to load
   * @return The loaded set of schedules
   */
  public static Props loadPropsInDir(final File dir, final String... suffixes) {
    return loadPropsInDir(null, dir, suffixes);
  }

  /**
   * Load job schedules from the given directories
   *
   * @param parent The parent properties for these properties
   * @param dir The directory to look in
   * @param suffixes File suffixes to load
   * @return The loaded set of schedules
   */
  public static Props loadPropsInDir(final Props parent, final File dir, final String... suffixes) {
    try {
      final Props props = new Props(parent);
      final File[] files = dir.listFiles();
      Arrays.sort(files);
      if (files != null) {
        for (final File f : files) {
          if (f.isFile() && endsWith(f, suffixes)) {
            props.putAll(new Props(null, f.getAbsolutePath()));
          }
        }
      }
      return props;
    } catch (final IOException e) {
      throw new RuntimeException("Error loading properties.", e);
    }
  }

  /**
   * Load Props
   *
   * @param parent parent prop
   * @param propFiles prop files
   * @return constructed new Prop
   */
  public static Props loadProps(final Props parent, final File... propFiles) {
    try {
      Props props = new Props(parent);
      for (final File f : propFiles) {
        if (f.isFile()) {
          props = new Props(props, f);
        }
      }

      return props;
    } catch (final IOException e) {
      throw new RuntimeException("Error loading properties.", e);
    }
  }

  /**
   * Load plugin properties
   *
   * @param pluginDir plugin's Base Directory
   * @return The properties
   */
  public static Props loadPluginProps(final File pluginDir) {
    if (!pluginDir.exists()) {
      LOG.error("Error! Plugin path " + pluginDir.getPath() + " doesn't exist.");
      return null;
    }

    if (!pluginDir.isDirectory()) {
      LOG.error("The plugin path " + pluginDir + " is not a directory.");
      return null;
    }

    final File propertiesDir = new File(pluginDir, "conf");
    if (propertiesDir.exists() && propertiesDir.isDirectory()) {
      final File propertiesFile = new File(propertiesDir, "plugin.properties");
      final File propertiesOverrideFile =
          new File(propertiesDir, "override.properties");

      if (propertiesFile.exists()) {
        if (propertiesOverrideFile.exists()) {
          return loadProps(null, propertiesFile, propertiesOverrideFile);
        } else {
          return loadProps(null, propertiesFile);
        }
      } else {
        LOG.error("Plugin conf file " + propertiesFile + " not found.");
        return null;
      }
    } else {
      LOG.error("Plugin conf path " + propertiesDir + " not found.");
      return null;
    }
  }

  /**
   * Load job schedules from the given directories
   *
   * @param dirs The directories to check for properties
   * @param suffixes The suffixes to load
   * @return The properties
   */
  public static Props loadPropsInDirs(final List<File> dirs, final String... suffixes) {
    final Props props = new Props();
    for (final File dir : dirs) {
      props.putLocal(loadPropsInDir(dir, suffixes));
    }
    return props;
  }

  /**
   * Load properties from the given path
   *
   * @param jobPath The path to load from
   * @param props The parent properties for loaded properties
   * @param suffixes The suffixes of files to load
   */
  public static void loadPropsBySuffix(final File jobPath, final Props props,
      final String... suffixes) {
    try {
      if (jobPath.isDirectory()) {
        final File[] files = jobPath.listFiles();
        if (files != null) {
          for (final File file : files) {
            loadPropsBySuffix(file, props, suffixes);
          }
        }
      } else if (endsWith(jobPath, suffixes)) {
        props.putAll(new Props(null, jobPath.getAbsolutePath()));
      }
    } catch (final IOException e) {
      throw new RuntimeException("Error loading schedule properties.", e);
    }
  }

  private static boolean endsWith(final File file, final String... suffixes) {
    for (final String suffix : suffixes) {
      if (file.getName().endsWith(suffix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if the prop value is a variable replacement pattern
   */
  public static boolean isVariableReplacementPattern(final String value) {
    final Matcher matcher = VARIABLE_REPLACEMENT_PATTERN.matcher(value);
    return matcher.matches();
  }

  /**
   * Resolve Props
   *
   * @param props props
   * @return resolved props
   */
  public static Props resolveProps(final Props props) {
    if (props == null) {
      return null;
    }

    final Props resolvedProps = new Props();

    final LinkedHashSet<String> visitedVariables = new LinkedHashSet<>();
    for (final String key : props.getKeySet()) {
      String value = props.get(key);
      if (value == null) {
        LOG.warn("Null value in props for key '" + key + "'. Replacing with empty string.");
        value = "";
      }

      visitedVariables.add(key);
      final String replacedValue =
          resolveVariableReplacement(value, props, visitedVariables);
      visitedVariables.clear();

      resolvedProps.put(key, replacedValue);
    }

    for (final String key : resolvedProps.getKeySet()) {
      final String value = resolvedProps.get(key);
      final String expressedValue = resolveVariableExpression(value);
      resolvedProps.put(key, expressedValue);
    }

    return resolvedProps;
  }

  private static String resolveVariableReplacement(final String value, final Props props,
      final LinkedHashSet<String> visitedVariables) {
    final StringBuffer buffer = new StringBuffer();
    int startIndex = 0;

    final Matcher matcher = VARIABLE_REPLACEMENT_PATTERN.matcher(value);
    while (matcher.find(startIndex)) {
      if (startIndex < matcher.start()) {
        // Copy everything up front to the buffer
        buffer.append(value.substring(startIndex, matcher.start()));
      }

      final String subVariable = matcher.group(1);
      // Detected a cycle
      if (visitedVariables.contains(subVariable)) {
        throw new IllegalArgumentException(String.format(
            "Circular variable substitution found: [%s] -> [%s]",
            StringUtils.join(visitedVariables, "->"), subVariable));
      } else {
        // Add substitute variable and recurse.
        final String replacement = props.get(subVariable);
        visitedVariables.add(subVariable);

        if (replacement == null) {
          throw new UndefinedPropertyException(String.format(
              "Could not find variable substitution for variable(s) [%s]",
              StringUtils.join(visitedVariables, "->")));
        }

        buffer.append(resolveVariableReplacement(replacement, props,
            visitedVariables));
        visitedVariables.remove(subVariable);
      }

      startIndex = matcher.end();
    }

    if (startIndex < value.length()) {
      buffer.append(value.substring(startIndex));
    }

    return buffer.toString();
  }

  private static String resolveVariableExpression(final String value) {
    final JexlEngine jexl = new JexlEngine();
    return resolveVariableExpression(value, value.length(), jexl);
  }

  /**
   * Function that looks for expressions to parse. It parses backwards to capture embedded
   * expressions
   */
  private static String resolveVariableExpression(final String value, final int last,
      final JexlEngine jexl) {
    final int lastIndex = value.lastIndexOf("$(", last);
    if (lastIndex == -1) {
      return value;
    }

    // Want to check that everything is well formed, and that
    // we properly capture $( ...(...)...).
    int bracketCount = 0;
    int nextClosed = lastIndex + 2;
    for (; nextClosed < value.length(); ++nextClosed) {
      if (value.charAt(nextClosed) == '(') {
        bracketCount++;
      } else if (value.charAt(nextClosed) == ')') {
        bracketCount--;
        if (bracketCount == -1) {
          break;
        }
      }
    }

    if (nextClosed == value.length()) {
      throw new IllegalArgumentException("Expression " + value
          + " not well formed.");
    }

    final String innerExpression = value.substring(lastIndex + 2, nextClosed);
    Object result = null;
    try {
      final Expression e = jexl.createExpression(innerExpression);
      result = e.evaluate(new MapContext());
    } catch (final JexlException e) {
      throw new IllegalArgumentException("Expression " + value
          + " not well formed. " + e.getMessage(), e);
    }

    if (result == null) {
      // for backward compatibility it is best to return value
      return value;
    }

    final String newValue =
        value.substring(0, lastIndex) + result.toString()
            + value.substring(nextClosed + 1);
    return resolveVariableExpression(newValue, lastIndex, jexl);
  }

  /**
   * Convert props to json string
   *
   * @param props props
   * @param localOnly include local prop sets only or not
   * @return json string format of props
   */
  public static String toJSONString(final Props props, final boolean localOnly) {
    final Map<String, String> map = toStringMap(props, localOnly);
    return JSONUtils.toJSON(map);
  }

  /**
   * Convert props to Map
   *
   * @param props props
   * @param localOnly include local prop sets only or not
   * @return String Map of props
   */
  public static Map<String, String> toStringMap(final Props props, final boolean localOnly) {
    final HashMap<String, String> map = new HashMap<>();
    final Set<String> keyset = localOnly ? props.localKeySet() : props.getKeySet();

    for (final String key : keyset) {
      final String value = props.get(key);
      map.put(key, value);
    }

    return map;
  }

  /**
   * Convert json String to Prop Object
   *
   * @param json json formatted string
   * @return a new constructed Prop Object
   * @throws IOException exception on parsing json string to prop object
   */
  public static Props fromJSONString(final String json) throws IOException {
    final Map<String, String> obj = (Map<String, String>) JSONUtils.parseJSONFromString(json);
    final Props props = new Props(null, obj);
    return props;
  }

  /**
   * Convert a hierarchical Map to Prop Object
   *
   * @param propsMap a hierarchical Map
   * @return a new constructed Props Object
   */
  public static Props fromHierarchicalMap(final Map<String, Object> propsMap) {
    if (propsMap == null) {
      return null;
    }

    final String source = (String) propsMap.get("source");
    final Map<String, String> propsParams =
        (Map<String, String>) propsMap.get("props");

    final Map<String, Object> parent = (Map<String, Object>) propsMap.get("parent");
    final Props parentProps = fromHierarchicalMap(parent);

    final Props props = new Props(parentProps, propsParams);
    props.setSource(source);
    return props;
  }

  /**
   * Convert a Props object to a hierarchical Map
   *
   * @param props props object
   * @return a hierarchical Map presented Props object
   */
  public static Map<String, Object> toHierarchicalMap(final Props props) {
    final Map<String, Object> propsMap = new HashMap<>();
    propsMap.put("source", props.getSource());
    propsMap.put("props", toStringMap(props, true));

    if (props.getParent() != null) {
      propsMap.put("parent", toHierarchicalMap(props.getParent()));
    }

    return propsMap;
  }

  /**
   * The difference between old and new Props
   *
   * @param oldProps old Props
   * @param newProps new Props
   * @return string formatted difference
   */
  public static String getPropertyDiff(Props oldProps, Props newProps) {

    final StringBuilder builder = new StringBuilder("");

    // oldProps can not be null during the below comparison process.
    if (oldProps == null) {
      oldProps = new Props();
    }

    if (newProps == null) {
      newProps = new Props();
    }

    final MapDifference<String, String> md =
        Maps.difference(toStringMap(oldProps, false), toStringMap(newProps, false));

    final Map<String, String> newlyCreatedProperty = md.entriesOnlyOnRight();
    if (newlyCreatedProperty != null && newlyCreatedProperty.size() > 0) {
      builder.append("Newly created Properties: ");
      newlyCreatedProperty.forEach((k, v) -> {
        builder.append("[ " + k + ", " + v + "], ");
      });
      builder.append("\n");
    }

    final Map<String, String> deletedProperty = md.entriesOnlyOnLeft();
    if (deletedProperty != null && deletedProperty.size() > 0) {
      builder.append("Deleted Properties: ");
      deletedProperty.forEach((k, v) -> {
        builder.append("[ " + k + ", " + v + "], ");
      });
      builder.append("\n");
    }

    final Map<String, MapDifference.ValueDifference<String>> diffProperties = md.entriesDiffering();
    if (diffProperties != null && diffProperties.size() > 0) {
      builder.append("Modified Properties: ");
      diffProperties.forEach((k, v) -> {
        builder.append("[ " + k + ", " + v.leftValue() + "-->" + v.rightValue() + "], ");
      });
    }
    return builder.toString();
  }
}
