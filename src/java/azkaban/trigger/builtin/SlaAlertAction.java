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

package azkaban.trigger.builtin;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorManagerAdapter;
import azkaban.sla.SlaOption;
import azkaban.trigger.TriggerAction;

public class SlaAlertAction implements TriggerAction{

	public static final String type = "AlertAction";
	
	private static final Logger logger = Logger.getLogger(SlaAlertAction.class);
	
	private String actionId;
	private SlaOption slaOption;
	private int execId;
//	private List<Map<String, Object>> alerts;
	private static Map<String, azkaban.alert.Alerter> alerters;
	private static ExecutorManagerAdapter executorManager;

	public SlaAlertAction(String id, SlaOption slaOption, int execId) {
		this.actionId = id;
		this.slaOption = slaOption;
		this.execId = execId;
//		this.alerts = alerts;
	}
	
	public static void setAlerters(Map<String, Alerter> alts) {
		alerters = alts;
	}
	
	public static void setExecutorManager(ExecutorManagerAdapter em) {
		executorManager = em;
	}
	
	@Override
	public String getId() {
		return actionId;
	}

	@Override
	public String getType() {
		return type;
	}

	@SuppressWarnings("unchecked")
	public static SlaAlertAction createFromJson(Object obj) throws Exception {
		return createFromJson((HashMap<String, Object>) obj);
	}
	
	public static SlaAlertAction createFromJson(HashMap<String, Object> obj) throws Exception {
		Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
		if(!jsonObj.get("type").equals(type)) {
			throw new Exception("Cannot create action of " + type + " from " + jsonObj.get("type"));
		}
		String actionId = (String) jsonObj.get("actionId");
		SlaOption slaOption = SlaOption.fromObject(jsonObj.get("slaOption"));
		int execId = Integer.valueOf((String) jsonObj.get("execId"));
//		List<Map<String, Object>> alerts = (List<Map<String, Object>>) jsonObj.get("alerts");
		return new SlaAlertAction(actionId, slaOption, execId);
	}
	
	@Override
	public TriggerAction fromJson(Object obj) throws Exception {
		return createFromJson(obj);
	}

	@Override
	public Object toJson() {
		Map<String, Object> jsonObj = new HashMap<String, Object>();
		jsonObj.put("actionId", actionId);
		jsonObj.put("type", type);
		jsonObj.put("slaOption", slaOption.toObject());
		jsonObj.put("execId", String.valueOf(execId));
//		jsonObj.put("alerts", alerts);

		return jsonObj;
	}

	@Override
	public void doAction() throws Exception {
//		for(Map<String, Object> alert : alerts) {
		logger.info("Alerting on sla failure.");
		Map<String, Object> alert = slaOption.getInfo();
			if(alert.containsKey(SlaOption.ALERT_TYPE)) {
				String alertType = (String) alert.get(SlaOption.ALERT_TYPE);
				Alerter alerter = alerters.get(alertType);
				if(alerter != null) {
					try {
						ExecutableFlow flow = executorManager.getExecutableFlow(execId);
						alerter.alertOnSla(slaOption, SlaOption.createSlaMessage(slaOption, flow));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						logger.error("Failed to alert by " + alertType);
					}
				}
				else {
					logger.error("Alerter type " + alertType + " doesn't exist. Failed to alert.");
				}
			}
//		}
	}

//	private String createSlaMessage() {
//		ExecutableFlow flow = null;
//		try {
//			flow = executorManager.getExecutableFlow(execId);
//		} catch (ExecutorManagerException e) {
//			e.printStackTrace();
//			logger.error("Failed to get executable flow.");
//		}
//		String type = slaOption.getType();
//		if(type.equals(SlaOption.TYPE_FLOW_FINISH)) {
//			String flowName = (String) slaOption.getInfo().get(SlaOption.INFO_FLOW_NAME);
//			String duration = (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
//			String basicinfo =  "SLA Alert: Your flow " + flowName + " failed to FINISH within " + duration + "</br>";
//			String expected = "Here is details : </br>" + "Flow " + flowName + " in execution " + execId + " is expected to FINISH within " + duration + " from " + flow.getStartTime() + "</br>"; 
//			String actual = "Actual flow status is " + flow.getStatus();
//			return basicinfo + expected + actual;
//		} else if(type.equals(SlaOption.TYPE_FLOW_SUCCEED)) {
//			String flowName = (String) slaOption.getInfo().get(SlaOption.INFO_FLOW_NAME);
//			String duration = (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
//			String basicinfo =  "SLA Alert: Your flow " + flowName + " failed to SUCCEED within " + duration + "</br>";
//			String expected = "Here is details : </br>" + "Flow " + flowName + " in execution " + execId + " expected to FINISH within " + duration + " from " + flow.getStartTime() + "</br>"; 
//			String actual = "Actual flow status is " + flow.getStatus();
//			return basicinfo + expected + actual;
//		} else if(type.equals(SlaOption.TYPE_JOB_FINISH)) {
//			String jobName = (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
//			String duration = (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
//			return "SLA Alert: Your job " + jobName + " failed to FINISH within " + duration + " in execution " + execId;
//		} else if(type.equals(SlaOption.TYPE_JOB_SUCCEED)) {
//			String jobName = (String) slaOption.getInfo().get(SlaOption.INFO_JOB_NAME);
//			String duration = (String) slaOption.getInfo().get(SlaOption.INFO_DURATION);
//			return "SLA Alert: Your job " + jobName + " failed to SUCCEED within " + duration + " in execution " + execId;
//		} else {
//			return "Unrecognized SLA type " + type;
//		}
//	}

	@Override
	public void setContext(Map<String, Object> context) {
	}

	@Override
	public String getDescription() {
		return type + " for " + execId + " with " + slaOption.toString();
	}

}
