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

import azkaban.executor.ExecutableFlowBase;
import azkaban.flow.CommonJobProperties;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import javax.print.attribute.standard.DateTimeAtCompleted;

public class PropsUtils {

	/**
	 * Load job schedules from the given directories ] * @param dir The
	 * directory to look in
	 * 
	 * @param suffixes
	 *            File suffixes to load
	 * @return The loaded set of schedules
	 */
	public static Props loadPropsInDir(File dir, String... suffixes) {
		return loadPropsInDir(null, dir, suffixes);
	}

	/**
	 * Load job schedules from the given directories
	 * 
	 * @param parent
	 *            The parent properties for these properties
	 * @param dir
	 *            The directory to look in
	 * @param suffixes
	 *            File suffixes to load
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

	public static Props loadProps(Props parent, File ... propFiles) {
		try {
			Props props = new Props(parent);
			for (File f: propFiles) {
				if (f.isFile()) {
					props = new Props(props, f);
				}
			}
			
			return props;
		}
		catch (IOException e) {
			throw new RuntimeException("Error loading properties.", e);
		}
	}
	
	/**
	 * Load job schedules from the given directories
	 * 
	 * @param dirs
	 *            The directories to check for properties
	 * @param suffixes
	 *            The suffixes to load
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
	 * @param jobPath
	 *            The path to load from
	 * @param props
	 *            The parent properties for loaded properties
	 * @param suffixes
	 *            The suffixes of files to load
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

	private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-zA-Z_.0-9]+)\\}");

	public static Props resolveProps(Props props) {
		if (props == null) return null;
		
		Props resolvedProps = new Props();
		
		LinkedHashSet<String> visitedVariables = new LinkedHashSet<String>();
		for (String key : props.getKeySet()) {
			String value = props.get(key);
			
			visitedVariables.add(key);
			String replacedValue = resolveVariableReplacement(value, props, visitedVariables);
			visitedVariables.clear();
			
			resolvedProps.put(key, replacedValue);
		}
		
		return resolvedProps;
	};
	
	private static String resolveVariableReplacement(String value, Props props, LinkedHashSet<String> visitedVariables) {
		StringBuffer buffer = new StringBuffer();
		int startIndex = 0;
		
		Matcher matcher = VARIABLE_PATTERN.matcher(value);
		while (matcher.find(startIndex)) {
			if (startIndex < matcher.start()) {
				// Copy everything up front to the buffer
				buffer.append(value.substring(startIndex, matcher.start()));
			}
			
			String subVariable = matcher.group(1);
			// Detected a cycle
			if (visitedVariables.contains(subVariable)) {
				throw new IllegalArgumentException(
						String.format("Circular variable substitution found: [%s] -> [%s]", 
								StringUtils.join(visitedVariables, "->"), subVariable));
			}
			else {
				// Add substitute variable and recurse.
				String replacement = props.get(subVariable);
				visitedVariables.add(subVariable);
				
				if (replacement == null) {
					throw new UndefinedPropertyException(
							String.format("Could not find variable substitution for variable(s) [%s]", 
									StringUtils.join(visitedVariables, "->")));
				}
				
				buffer.append(resolveVariableReplacement(replacement, props, visitedVariables));
				visitedVariables.remove(subVariable);
			}
			
			startIndex = matcher.end();
		}
		
		if (startIndex < value.length()) {
			buffer.append(value.substring(startIndex));
		}
		
		return buffer.toString();
	}
	
	public static Props addCommonFlowProperties(Props parentProps, final ExecutableFlowBase flow) {
		Props props = new Props(parentProps);

		props.put(CommonJobProperties.FLOW_ID, flow.getFlowId());
		props.put(CommonJobProperties.EXEC_ID, flow.getExecutionId());
		props.put(CommonJobProperties.PROJECT_ID, flow.getProjectId());
		props.put(CommonJobProperties.PROJECT_VERSION, flow.getVersion());
		props.put(CommonJobProperties.FLOW_UUID, UUID.randomUUID().toString());

		DateTime loadTime = new DateTime();
        DateTime hourAgo = loadTime.minusHours(1);
        DateTime dayAgo = loadTime.minusDays(1);
        DateTime weekAgo = loadTime.minusWeeks(1);
        DateTime monthAgo = loadTime.minusMonths(1);
        DateTime yearAgo = loadTime.minusYears(1);

		props.put(CommonJobProperties.FLOW_START_TIMESTAMP, loadTime.toString());
		props.put(CommonJobProperties.FLOW_START_YEAR, loadTime.toString("yyyy"));
		props.put(CommonJobProperties.FLOW_START_MONTH, loadTime.toString("MM"));
		props.put(CommonJobProperties.FLOW_START_DAY, loadTime.toString("dd"));
		props.put(CommonJobProperties.FLOW_START_HOUR, loadTime.toString("HH"));
        props.put(CommonJobProperties.FLOW_START_MINUTE, loadTime.toString("mm"));
		props.put(CommonJobProperties.FLOW_START_SECOND, loadTime.toString("ss"));
		props.put(CommonJobProperties.FLOW_START_MILLISSECOND, loadTime.toString("SSS"));
		props.put(CommonJobProperties.FLOW_START_TIMEZONE, loadTime.toString("ZZZZ"));

        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_TIMESTAMP, hourAgo.toString());
        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_YEAR, hourAgo.toString("yyyy"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_MONTH, hourAgo.toString("MM"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_DAY, hourAgo.toString("dd"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_HOUR, hourAgo.toString("HH"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_MINUTE, hourAgo.toString("mm"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_SECOND, hourAgo.toString("ss"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_MILLISSECOND, hourAgo.toString("SSS"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_HOUR_TIMEZONE, hourAgo.toString("ZZZZ"));

        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_TIMESTAMP, dayAgo.toString());
        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_YEAR, dayAgo.toString("yyyy"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_MONTH, dayAgo.toString("MM"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_DAY, dayAgo.toString("dd"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_HOUR, dayAgo.toString("HH"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_MINUTE, dayAgo.toString("mm"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_SECOND, dayAgo.toString("ss"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_MILLISSECOND, dayAgo.toString("SSS"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_DAY_TIMEZONE, dayAgo.toString("ZZZZ"));

        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_TIMESTAMP, weekAgo.toString());
        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_YEAR, weekAgo.toString("yyyy"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_MONTH, weekAgo.toString("MM"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_DAY, weekAgo.toString("dd"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_HOUR, weekAgo.toString("HH"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_MINUTE, weekAgo.toString("mm"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_SECOND, weekAgo.toString("ss"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_MILLISSECOND, weekAgo.toString("SSS"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_WEEK_TIMEZONE, weekAgo.toString("ZZZZ"));

        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_TIMESTAMP, monthAgo.toString());
        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_YEAR, monthAgo.toString("yyyy"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_MONTH, monthAgo.toString("MM"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_DAY, monthAgo.toString("dd"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_HOUR, monthAgo.toString("HH"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_MINUTE, monthAgo.toString("mm"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_SECOND, monthAgo.toString("ss"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_MILLISSECOND, monthAgo.toString("SSS"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_MONTH_TIMEZONE, monthAgo.toString("ZZZZ"));

        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_TIMESTAMP, yearAgo.toString());
        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_YEAR, yearAgo.toString("yyyy"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_MONTH, yearAgo.toString("MM"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_DAY, yearAgo.toString("dd"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_HOUR, yearAgo.toString("HH"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_MINUTE, yearAgo.toString("mm"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_SECOND, yearAgo.toString("ss"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_MILLISSECOND, yearAgo.toString("SSS"));
        props.put(CommonJobProperties.FLOW_BACK_ONE_YEAR_TIMEZONE, yearAgo.toString("ZZZZ"));

		return props;
	}

	public static String toJSONString(Props props, boolean localOnly) {
		Map<String, String> map = toStringMap(props, localOnly);
		return JSONUtils.toJSON(map);
	}

	public static Map<String, String> toStringMap(Props props, boolean localOnly) {
		HashMap<String, String> map = new HashMap<String, String>();
		Set<String> keyset = localOnly ? props.localKeySet() : props.getKeySet();
		
		for (String key: keyset) {
			String value = props.get(key);
			map.put(key, value);
		}
		
		return map;
	}
	
	@SuppressWarnings("unchecked")
	public static Props fromJSONString(String json) {
		try {
			Map<String, String> obj = (Map<String, String>)JSONUtils.parseJSONFromString(json);
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
		
		String source = (String)propsMap.get("source");
		Map<String, String> propsParams = (Map<String,String>)propsMap.get("props");
		
		Map<String,Object> parent = (Map<String,Object>)propsMap.get("parent");
		Props parentProps = fromHierarchicalMap(parent);
		
		Props props = new Props(parentProps, propsParams);
		props.setSource(source);
		return props;
	}
	
	public static Map<String,Object> toHierarchicalMap(Props props) {
		Map<String,Object> propsMap = new HashMap<String,Object>();
		propsMap.put("source", props.getSource());
		propsMap.put("props", toStringMap(props, true));
		
		if (props.getParent() != null) {
			propsMap.put("parent", toHierarchicalMap(props.getParent()));
		}
		
		return propsMap;
	}
}
