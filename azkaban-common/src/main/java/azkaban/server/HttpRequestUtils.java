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
package azkaban.server;

import static azkaban.executor.ExecutionOptions.FAILURE_ACTION_OVERRIDE;

import azkaban.Constants;
import azkaban.Constants.FlowParameters;
import azkaban.executor.DisabledJob;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.ExecutorManagerException;
import azkaban.sla.SlaOption;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.utils.JSONUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

public class HttpRequestUtils {

  public static final String PARAM_SLA_SETTINGS = "slaSettings";

  public static ExecutionOptions parseFlowOptions(final HttpServletRequest req, final String flowName)
      throws ServletException {
    final ExecutionOptions execOptions = new ExecutionOptions();

    if (hasParam(req, "failureAction")) {
      final String option = getParam(req, "failureAction");
      if (option.equals("finishCurrent")) {
        execOptions.setFailureAction(FailureAction.FINISH_CURRENTLY_RUNNING);
      } else if (option.equals("cancelImmediately")) {
        execOptions.setFailureAction(FailureAction.CANCEL_ALL);
      } else if (option.equals("finishPossible")) {
        execOptions.setFailureAction(FailureAction.FINISH_ALL_POSSIBLE);
      }
    }

    if (hasParam(req, "failureEmailsOverride")) {
      final boolean override = getBooleanParam(req, "failureEmailsOverride", false);
      execOptions.setFailureEmailsOverridden(override);
    }
    if (hasParam(req, "successEmailsOverride")) {
      final boolean override = getBooleanParam(req, "successEmailsOverride", false);
      execOptions.setSuccessEmailsOverridden(override);
    }

    if (hasParam(req, FAILURE_ACTION_OVERRIDE)) {
      final boolean override = getBooleanParam(req, FAILURE_ACTION_OVERRIDE, false);
      execOptions.setFailureActionOverride(override);
    }

    if (hasParam(req, "failureEmails")) {
      final String emails = getParam(req, "failureEmails");
      if (!emails.isEmpty()) {
        final String[] emailSplit = emails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
        execOptions.setFailureEmails(Arrays.asList(emailSplit));
      }
    }
    if (hasParam(req, "successEmails")) {
      final String emails = getParam(req, "successEmails");
      if (!emails.isEmpty()) {
        final String[] emailSplit = emails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
        execOptions.setSuccessEmails(Arrays.asList(emailSplit));
      }
    }
    if (hasParam(req, "notifyFailureFirst")) {
      execOptions.setNotifyOnFirstFailure(Boolean.parseBoolean(getParam(req,
          "notifyFailureFirst")));
    }
    if (hasParam(req, "notifyFailureLast")) {
      execOptions.setNotifyOnLastFailure(Boolean.parseBoolean(getParam(req,
          "notifyFailureLast")));
    }

    final String concurrentOption = getParam(req, "concurrentOption", "skip");
    execOptions.setConcurrentOption(concurrentOption);
    if (concurrentOption.equals("pipeline")) {
      final int pipelineLevel = getIntParam(req, "pipelineLevel");
      execOptions.setPipelineLevel(pipelineLevel);
    } else if (concurrentOption.equals("queue")) {
      // Not yet implemented
      final int queueLevel = getIntParam(req, "queueLevel", 1);
      execOptions.setPipelineLevel(queueLevel);
    }

    if (hasParam(req, "mailCreator")) {
      final String mailCreator = getParam(req, "mailCreator");
      execOptions.setMailCreator(mailCreator);
    }

    final List<SlaOption> slaOptions = SlaRequestUtils.parseSlaOptions(req, flowName, PARAM_SLA_SETTINGS);
    if (!slaOptions.isEmpty()) {
      execOptions.setSlaOptions(slaOptions);
    }

    final Map<String, Map<String, String>> runtimePropertiesGroup = getMapParamGroup(req,
        "runtimeProperty");

    // legacy support
    final Map<String, String> flowParamGroup = getParamGroup(req, "flowOverride");

    // Don't allow combining old & new in the same request:
    // This ensures that there's no need to handle possible conflicts between flowOverride &
    // runtimeProperty[ROOT]
    if (!flowParamGroup.isEmpty() && !runtimePropertiesGroup.isEmpty()) {
      throw new ServletException("The legacy param group flowOverride is not allowed in "
          + "combination with the runtimeProperty param group. "
          + "Migrate flowOverride to runtimeProperty[ROOT].");
    }

    // Add all flow-level props – they are accessed via ExecutionOptions#getFlowParameters.
    final Map<String, String> rootProps = runtimePropertiesGroup
        .remove(Constants.ROOT_NODE_IDENTIFIER);
    if (rootProps != null) {
      flowParamGroup.putAll(rootProps);
    }

    // The ROOT runtime properties
    execOptions.addAllFlowParameters(flowParamGroup);
    // Any other runtime properties
    execOptions.addAllRuntimeProperties(runtimePropertiesGroup);

    if (hasParam(req, "disabled")) {
      final String disabled = getParam(req, "disabled");
      if (!disabled.isEmpty()) {
        // TODO edlu: see if it's possible to pass in the new format
        final List<DisabledJob> disabledList =
            DisabledJob.fromDeprecatedObjectList((List<Object>) JSONUtils
                .parseJSONFromStringQuiet(disabled));
        execOptions.setDisabledJobs(disabledList);
      }
    }
    return execOptions;
  }

  /**
   * <pre>
   * Remove following flow param if submitting user is not an Azkaban admin
   * FLOW_PRIORITY
   * USE_EXECUTOR
   * @param userManager
   * @param options
   * @param user
   * </pre>
   */
  public static void filterAdminOnlyFlowParams(final UserManager userManager,
      final ExecutionOptions options, final User user) throws ExecutorManagerException {
    if (options == null || options.getFlowParameters() == null) {
      return;
    }

    final Map<String, String> params = options.getFlowParameters();
    // is azkaban Admin
    if (!hasPermission(userManager, user, Type.ADMIN)) {
      params.remove(ExecutionOptions.FLOW_PRIORITY);
      params.remove(ExecutionOptions.USE_EXECUTOR);
      params.remove(FlowParameters.FLOW_PARAM_JAVA_ENABLE_DEBUG);
      params.remove(FlowParameters.FLOW_PARAM_ENABLE_DEV_POD);
      params.remove(FlowParameters.FLOW_PARAM_DISABLE_POD_CLEANUP);
      // Passing test version will be allowed for Azkaban ADMIN role only
      params.remove(FlowParameters.FLOW_PARAM_ALLOW_IMAGE_TEST_VERSION);
    } else {
      validateIntegerParam(params, ExecutionOptions.FLOW_PRIORITY);
      validateIntegerParam(params, ExecutionOptions.USE_EXECUTOR);
      validateBooleanParam(params, FlowParameters.FLOW_PARAM_JAVA_ENABLE_DEBUG);
      validateBooleanParam(params, FlowParameters.FLOW_PARAM_ENABLE_DEV_POD);
      validateBooleanParam(params, FlowParameters.FLOW_PARAM_DISABLE_POD_CLEANUP);
      // Passing of test version is allowed for azkaban admin only. Validate
      // if it is boolean param
      validateBooleanParam(params, FlowParameters.FLOW_PARAM_ALLOW_IMAGE_TEST_VERSION);
    }
  }

  /**
   * parse a string as number and throws exception if parsed value is not a valid integer
   *
   * @throws ExecutorManagerException if paramName is not a valid integer
   */
  public static boolean validateIntegerParam(final Map<String, String> params,
      final String paramName) throws ExecutorManagerException {
    if (params != null && params.containsKey(paramName)
        && !StringUtils.isNumeric(params.get(paramName))) {
      throw new ExecutorManagerException(paramName + " should be an integer");
    }
    return true;
  }

  /**
   * Parse a string as boolean and throws exception if parsed value is not a valid boolean.
   *
   * @param params
   * @param paramName
   * @return
   * @throws ExecutorManagerException
   */
  public static boolean validateBooleanParam(final Map<String, String> params,
      final String paramName) throws ExecutorManagerException {
    if (params != null && params.containsKey(paramName) && null ==
        BooleanUtils.toBooleanObject(params.get(paramName))) {
      throw new ExecutorManagerException(paramName + " should be boolean");
    }
    return true;
  }

  /**
   * returns true if user has access of type
   */
  public static boolean hasPermission(final UserManager userManager, final User user,
      final Permission.Type type) {
    for (final String roleName : user.getRoles()) {
      final Role role = userManager.getRole(roleName);
      if (role.getPermission().isPermissionSet(type)
          || role.getPermission().isPermissionSet(Permission.Type.ADMIN)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks for the existance of the parameter in the request
   */
  public static boolean hasParam(final HttpServletRequest request, final String param) {
    return request.getParameter(param) != null;
  }

  /**
   * Retrieves the param from the http servlet request. Will throw an exception if not found
   */
  public static String getParam(final HttpServletRequest request, final String name)
      throws ServletException {
    final String p = request.getParameter(name);
    if (p == null) {
      throw new ServletException("Missing required parameter '" + name + "'.");
    } else {
      return p;
    }
  }

  /**
   * Retrieves the param from the http servlet request.
   */
  public static String getParam(final HttpServletRequest request, final String name,
      final String defaultVal) {
    final String p = request.getParameter(name);
    if (p == null) {
      return defaultVal;
    }
    return p;
  }

  /**
   * Returns the param and parses it into an int. Will throw an exception if not found, or a parse
   * error if the type is incorrect.
   */
  public static int getIntParam(final HttpServletRequest request, final String name)
      throws ServletException {
    final String p = getParam(request, name);
    return Integer.parseInt(p);
  }

  public static int getIntParam(final HttpServletRequest request, final String name,
      final int defaultVal) {
    if (hasParam(request, name)) {
      try {
        return getIntParam(request, name);
      } catch (final Exception e) {
        return defaultVal;
      }
    }

    return defaultVal;
  }

  public static boolean getBooleanParam(final HttpServletRequest request, final String name)
      throws ServletException {
    final String p = getParam(request, name);
    return Boolean.parseBoolean(p);
  }

  public static boolean getBooleanParam(final HttpServletRequest request,
      final String name, final boolean defaultVal) {
    if (hasParam(request, name)) {
      try {
        return getBooleanParam(request, name);
      } catch (final Exception e) {
        return defaultVal;
      }
    }

    return defaultVal;
  }

  public static long getLongParam(final HttpServletRequest request, final String name)
      throws ServletException {
    final String p = getParam(request, name);
    return Long.valueOf(p);
  }

  public static long getLongParam(final HttpServletRequest request, final String name,
      final long defaultVal) {
    if (hasParam(request, name)) {
      try {
        return getLongParam(request, name);
      } catch (final Exception e) {
        return defaultVal;
      }
    }

    return defaultVal;
  }

  /**
   * Read params like groupName[key]: value
   */
  public static Map<String, String> getParamGroup(final HttpServletRequest request,
      final String groupName) throws ServletException {
    final Enumeration<String> enumerate = request.getParameterNames();
    final String matchString = groupName + "[";

    final HashMap<String, String> groupParam = new HashMap<>();
    while (enumerate.hasMoreElements()) {
      final String str = enumerate.nextElement();
      if (str.startsWith(matchString)) {
        try {
          groupParam.put(str.substring(matchString.length(), str.length() - 1),
              request.getParameter(str));
        } catch (IndexOutOfBoundsException e) {
          throw new ServletException(String.format("Param group %s is invalid.", groupName), e);
        }
      }

    }
    return groupParam;
  }

  /**
   * Read params like groupName[level1Key][level2Key]: value
   */
  public static Map<String, Map<String, String>> getMapParamGroup(final HttpServletRequest request,
      final String groupName) throws ServletException {
    final Enumeration<String> enumerate = request.getParameterNames();
    final String matchString = groupName + "[";

    final Map<String, Map<String, String>> groupParam = new HashMap<>();
    while (enumerate.hasMoreElements()) {
      final String str = enumerate.nextElement();
      if (str.startsWith(matchString)) {
        try {
          final int level1KeyEnd = str.indexOf("]");

          final String level1Key = str.substring(matchString.length(), level1KeyEnd);
          groupParam.putIfAbsent(level1Key, new HashMap<>());

          final String level2Key = str.substring(level1KeyEnd + 2, str.length() - 1);
          groupParam.get(level1Key).put(level2Key, request.getParameter(str));
        } catch (IndexOutOfBoundsException e) {
          throw new ServletException(String.format("Param group %s is invalid.", groupName), e);
        }
      }

    }
    return groupParam;
  }

  public static Object getJsonBody(final HttpServletRequest request) throws ServletException {
    try {
      return JSONUtils.parseJSONFromString(getBody(request));
    } catch (final IOException e) {
      throw new ServletException("HTTP Request JSON Body cannot be parsed.", e);
    }
  }

  public static String getBody(final HttpServletRequest request) throws ServletException {
    try {
      final StringBuffer stringBuffer = new StringBuffer();
      String line = null;
      final BufferedReader reader = request.getReader();
      while ((line = reader.readLine()) != null) {
        stringBuffer.append(line);
      }
      return stringBuffer.toString();
    } catch (final Exception e) {
      throw new ServletException("HTTP Request Body cannot be parsed.", e);
    }
  }
}
