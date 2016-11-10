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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.lang.StringUtils;

import org.joda.time.DateTime;

import azkaban.executor.ExecutableFlowBase;
import azkaban.flow.CommonJobProperties;

public class PropsUtils {

  /**
   * Load job schedules from the given directories ] * @param dir The directory
   * to look in
   *
   * @param suffixes File suffixes to load
   * @return The loaded set of schedules
   */
  public static Props loadPropsInDir(File dir, String... suffixes) {
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
  public static Props loadPropsInDir(Props parent, File dir, String... suffixes) {
    try {
      Props props = new Props(parent);
      File[] files = dir.listFiles();
      Arrays.sort(files);
      if (files != null) {
        for (File f : files) {
          if (f.isFile() && endsWith(f, suffixes)) {
            props.putAll(new Props(null, f.getAbsolutePath()));
          }
        }
      }
      return props;
    } catch (IOException e) {
      throw new RuntimeException("Error loading properties.", e);
    }
  }

  public static Props loadProps(Props parent, File... propFiles) {
    try {
      Props props = new Props(parent);
      for (File f : propFiles) {
        if (f.isFile()) {
          props = new Props(props, f);
        }
      }

      return props;
    } catch (IOException e) {
      throw new RuntimeException("Error loading properties.", e);
    }
  }

  /**
   * Load job schedules from the given directories
   *
   * @param dirs The directories to check for properties
   * @param suffixes The suffixes to load
   * @return The properties
   */
  public static Props loadPropsInDirs(List<File> dirs, String... suffixes) {
    Props props = new Props();
    for (File dir : dirs) {
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
  public static void loadPropsBySuffix(File jobPath, Props props,
      String... suffixes) {
    try {
      if (jobPath.isDirectory()) {
        File[] files = jobPath.listFiles();
        if (files != null) {
          for (File file : files)
            loadPropsBySuffix(file, props, suffixes);
        }
      } else if (endsWith(jobPath, suffixes)) {
        props.putAll(new Props(null, jobPath.getAbsolutePath()));
      }
    } catch (IOException e) {
      throw new RuntimeException("Error loading schedule properties.", e);
    }
  }

  public static boolean endsWith(File file, String... suffixes) {
    for (String suffix : suffixes)
      if (file.getName().endsWith(suffix))
        return true;
    return false;
  }

  private static final Pattern VARIABLE_REPLACEMENT_PATTERN = Pattern
      .compile("\\$\\{([a-zA-Z_.0-9]+)\\}");

  public static boolean isVarialbeReplacementPattern(String str) {
    Matcher matcher = VARIABLE_REPLACEMENT_PATTERN.matcher(str);
    return matcher.matches();
  }

  public static Props resolveProps(Props props) {
    if (props == null)
      return null;

    Props resolvedProps = new Props();

    LinkedHashSet<String> visitedVariables = new LinkedHashSet<String>();
    for (String key : props.getKeySet()) {
      String value = props.get(key);

      visitedVariables.add(key);
      String replacedValue =
          resolveVariableReplacement(value, props, visitedVariables);
      visitedVariables.clear();

      resolvedProps.put(key, replacedValue);
    }

    for (String key : resolvedProps.getKeySet()) {
      String value = resolvedProps.get(key);
      String expressedValue = resolveVariableExpression(value);
      resolvedProps.put(key, expressedValue);
    }

    return resolvedProps;
  };

  private static String resolveVariableReplacement(String value, Props props,
      LinkedHashSet<String> visitedVariables) {
    StringBuffer buffer = new StringBuffer();
    int startIndex = 0;

    Matcher matcher = VARIABLE_REPLACEMENT_PATTERN.matcher(value);
    while (matcher.find(startIndex)) {
      if (startIndex < matcher.start()) {
        // Copy everything up front to the buffer
        buffer.append(value.substring(startIndex, matcher.start()));
      }

      String subVariable = matcher.group(1);
      // Detected a cycle
      if (visitedVariables.contains(subVariable)) {
        throw new IllegalArgumentException(String.format(
            "Circular variable substitution found: [%s] -> [%s]",
            StringUtils.join(visitedVariables, "->"), subVariable));
      } else {
        // Add substitute variable and recurse.
        String replacement = props.get(subVariable);
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

  private static String resolveVariableExpression(String value) {
    JexlEngine jexl = new JexlEngine();
    return resolveVariableExpression(value, value.length(), jexl);
  }

  /**
   * Function that looks for expressions to parse. It parses backwards to
   * capture embedded expressions
   *
   * @param value
   * @param last
   * @param jexl
   * @return
   */
  private static String resolveVariableExpression(String value, int last,
      JexlEngine jexl) {
    int lastIndex = value.lastIndexOf("$(", last);
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

    String innerExpression = value.substring(lastIndex + 2, nextClosed);
    Object result = null;
    try {
      Expression e = jexl.createExpression(innerExpression);
      result = e.evaluate(new MapContext());
    } catch (JexlException e) {
      throw new IllegalArgumentException("Expression " + value
          + " not well formed. " + e.getMessage(), e);
    }

    if (result == null) {
      // for backward compatibility it is best to return value
      return value;
    }

    String newValue =
        value.substring(0, lastIndex) + result.toString()
            + value.substring(nextClosed + 1);
    return resolveVariableExpression(newValue, lastIndex, jexl);
  }

  public static Props addCommonFlowProperties(Props parentProps,
      final ExecutableFlowBase flow) {
    Props props = new Props(parentProps);

    props.put(CommonJobProperties.FLOW_ID, flow.getFlowId());
    props.put(CommonJobProperties.EXEC_ID, flow.getExecutionId());
    props.put(CommonJobProperties.PROJECT_ID, flow.getProjectId());
    props.put(CommonJobProperties.PROJECT_NAME, flow.getProjectName());
    props.put(CommonJobProperties.PROJECT_VERSION, flow.getVersion());
    props.put(CommonJobProperties.FLOW_UUID, UUID.randomUUID().toString());
    props.put(CommonJobProperties.PROJECT_LAST_CHANGED_BY, flow.getLastModifiedByUser());
    props.put(CommonJobProperties.PROJECT_LAST_CHANGED_DATE, flow.getLastModifiedTimestamp());
    props.put(CommonJobProperties.SUBMIT_USER, flow.getExecutableFlow().getSubmitUser());  

    DateTime loadTime = new DateTime();

    props.put(CommonJobProperties.FLOW_START_TIMESTAMP, loadTime.toString());
    props.put(CommonJobProperties.FLOW_START_YEAR, loadTime.toString("yyyy"));
    props.put(CommonJobProperties.FLOW_START_MONTH, loadTime.toString("MM"));
    props.put(CommonJobProperties.FLOW_START_DAY, loadTime.toString("dd"));
    props.put(CommonJobProperties.FLOW_START_HOUR, loadTime.toString("HH"));
    props.put(CommonJobProperties.FLOW_START_MINUTE, loadTime.toString("mm"));
    props.put(CommonJobProperties.FLOW_START_SECOND, loadTime.toString("ss"));
    props.put(CommonJobProperties.FLOW_START_MILLISSECOND,
        loadTime.toString("SSS"));
    props.put(CommonJobProperties.FLOW_START_TIMEZONE,
        loadTime.toString("ZZZZ"));

    return props;
  }

  public static String toJSONString(Props props, boolean localOnly) {
    Map<String, String> map = toStringMap(props, localOnly);
    return JSONUtils.toJSON(map);
  }

  public static Map<String, String> toStringMap(Props props, boolean localOnly) {
    HashMap<String, String> map = new HashMap<String, String>();
    Set<String> keyset = localOnly ? props.localKeySet() : props.getKeySet();

    for (String key : keyset) {
      String value = props.get(key);
      map.put(key, value);
    }

    return map;
  }

  @SuppressWarnings("unchecked")
  public static Props fromJSONString(String json) {
    try {
      Map<String, String> obj =
          (Map<String, String>) JSONUtils.parseJSONFromString(json);
      Props props = new Props(null, obj);
      return props;
    } catch (IOException e) {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static Props fromHierarchicalMap(Map<String, Object> propsMap) {
    if (propsMap == null) {
      return null;
    }

    String source = (String) propsMap.get("source");
    Map<String, String> propsParams =
        (Map<String, String>) propsMap.get("props");

    Map<String, Object> parent = (Map<String, Object>) propsMap.get("parent");
    Props parentProps = fromHierarchicalMap(parent);

    Props props = new Props(parentProps, propsParams);
    props.setSource(source);
    return props;
  }

  public static Map<String, Object> toHierarchicalMap(Props props) {
    Map<String, Object> propsMap = new HashMap<String, Object>();
    propsMap.put("source", props.getSource());
    propsMap.put("props", toStringMap(props, true));

    if (props.getParent() != null) {
      propsMap.put("parent", toHierarchicalMap(props.getParent()));
    }

    return propsMap;
  }
}
