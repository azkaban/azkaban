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

package azkaban.webapp.servlet;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.executor.mail.DefaultMailCreator;
import azkaban.utils.JSONUtils;

public class HttpRequestUtils {
	public static ExecutionOptions parseFlowOptions(HttpServletRequest req) throws ServletException {
		ExecutionOptions execOptions = new ExecutionOptions();
		
		if (hasParam(req, "failureAction")) {
			String option = getParam(req, "failureAction");
			if (option.equals("finishCurrent") ) {
				execOptions.setFailureAction(FailureAction.FINISH_CURRENTLY_RUNNING);
			}
			else if (option.equals("cancelImmediately")) {
				execOptions.setFailureAction(FailureAction.CANCEL_ALL);
			}
			else if (option.equals("finishPossible")) {
				execOptions.setFailureAction(FailureAction.FINISH_ALL_POSSIBLE);
			}
		}

		if (hasParam(req, "failureEmailsOverride")) {
			boolean override = getBooleanParam(req, "failureEmailsOverride", false);
			execOptions.setFailureEmailsOverridden(override);
		}
		if (hasParam(req, "successEmailsOverride")) {
			boolean override = getBooleanParam(req, "successEmailsOverride", false);
			execOptions.setSuccessEmailsOverridden(override);
		}

		if (hasParam(req, "failureEmails")) {
			String emails = getParam(req, "failureEmails");
			if (!emails.isEmpty()) {
				String[] emailSplit = emails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
				execOptions.setFailureEmails(Arrays.asList(emailSplit));
			}
		}
		if (hasParam(req, "successEmails")) {
			String emails = getParam(req, "successEmails");
			if (!emails.isEmpty()) {
				String[] emailSplit = emails.split("\\s*,\\s*|\\s*;\\s*|\\s+");
				execOptions.setSuccessEmails(Arrays.asList(emailSplit));
			}
		}
		if (hasParam(req, "notifyFailureFirst")) {
			execOptions.setNotifyOnFirstFailure(Boolean.parseBoolean(getParam(req, "notifyFailureFirst")));
		}
		if (hasParam(req, "notifyFailureLast")) {
			execOptions.setNotifyOnLastFailure(Boolean.parseBoolean(getParam(req, "notifyFailureLast")));
		}

		String concurrentOption = "skip";
		if (hasParam(req, "concurrentOption")) {
			concurrentOption = getParam(req, "concurrentOption");
			execOptions.setConcurrentOption(concurrentOption);
			if (concurrentOption.equals("pipeline")) {
				int pipelineLevel = getIntParam(req, "pipelineLevel");
				execOptions.setPipelineLevel(pipelineLevel);
			}
			else if (concurrentOption.equals("queue")) {
				// Not yet implemented
				int queueLevel = getIntParam(req, "queueLevel", 1);
				execOptions.setPipelineLevel(queueLevel);
			}
		}
		String mailCreator = DefaultMailCreator.DEFAULT_MAIL_CREATOR;
		if (hasParam(req, "mailCreator")) {
			mailCreator = getParam(req, "mailCreator");
			execOptions.setMailCreator(mailCreator);
		}
		
		Map<String, String> flowParamGroup = getParamGroup(req, "flowOverride");
		execOptions.addAllFlowParameters(flowParamGroup);
		
		if (hasParam(req, "disabled")) {
			String disabled = getParam(req, "disabled");
			if (!disabled.isEmpty()) {
				@SuppressWarnings("unchecked")
				List<Object> disabledList = (List<Object>)JSONUtils.parseJSONFromStringQuiet(disabled);
				execOptions.setDisabledJobs(disabledList);
			}
		}
		return execOptions;
	}
	
	/**
	 * Checks for the existance of the parameter in the request
	 * 
	 * @param request
	 * @param param
	 * @return
	 */
	public static boolean hasParam(HttpServletRequest request, String param) {
		return request.getParameter(param) != null;
	}

	/**
	 * Retrieves the param from the http servlet request. Will throw an
	 * exception if not found
	 * 
	 * @param request
	 * @param name
	 * @return
	 * @throws ServletException
	 */
	public static String getParam(HttpServletRequest request, String name) throws ServletException {
		String p = request.getParameter(name);
		if (p == null) {
			throw new ServletException("Missing required parameter '" + name + "'.");
		}
		else {
			return p;
		}
	}

	/**
	 * Retrieves the param from the http servlet request.
	 * 
	 * @param request
	 * @param name
	 * @param default
	 * 
	 * @return
	 */
	public static String getParam(HttpServletRequest request, String name, String defaultVal){
		String p = request.getParameter(name);
		if (p == null) {
			return defaultVal;
		}
		return p;
	}

	
	/**
	 * Returns the param and parses it into an int. Will throw an exception if
	 * not found, or a parse error if the type is incorrect.
	 * 
	 * @param request
	 * @param name
	 * @return
	 * @throws ServletException
	 */
	public static int getIntParam(HttpServletRequest request, String name) throws ServletException {
		String p = getParam(request, name);
		return Integer.parseInt(p);
	}
	
	public static int getIntParam(HttpServletRequest request, String name, int defaultVal) {
		if (hasParam(request, name)) {
			try {
				return getIntParam(request, name);
			} catch (Exception e) {
				return defaultVal;
			}
		}
		
		return defaultVal;
	}

	public static boolean getBooleanParam(HttpServletRequest request, String name) throws ServletException  {
		String p = getParam(request, name);
		return Boolean.parseBoolean(p);
	}
	
	public static boolean getBooleanParam(HttpServletRequest request, String name, boolean defaultVal) {
		if (hasParam(request, name)) {
			try {
				return getBooleanParam(request, name);
			} catch (Exception e) {
				return defaultVal;
			}
		}
		
		return defaultVal;
	}
	
	public static long getLongParam(HttpServletRequest request, String name) throws ServletException {
		String p = getParam(request, name);
		return Long.valueOf(p);
	}
	
	public static long getLongParam(HttpServletRequest request, String name, long defaultVal) {
		if (hasParam(request, name)) {
			try {
				return getLongParam(request, name);
			} catch (Exception e) {
				return defaultVal;
			}
		}
		
		return defaultVal;
	}

	
	public static Map<String, String> getParamGroup(HttpServletRequest request, String groupName)  throws ServletException {
		@SuppressWarnings("unchecked")
		Enumeration<Object> enumerate = (Enumeration<Object>)request.getParameterNames();
		String matchString = groupName + "[";

		HashMap<String, String> groupParam = new HashMap<String, String>();
		while( enumerate.hasMoreElements() ) {
			String str = (String)enumerate.nextElement();
			if (str.startsWith(matchString)) {
				groupParam.put(str.substring(matchString.length(), str.length() - 1), request.getParameter(str));
			}
			
		}
		return groupParam;
	}
	
}
