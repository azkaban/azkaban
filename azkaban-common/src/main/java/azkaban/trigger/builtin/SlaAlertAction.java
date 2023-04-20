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

import azkaban.ServiceProvider;
import azkaban.alert.Alerter;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutorLoader;
import azkaban.sla.SlaOption;
import azkaban.trigger.TriggerAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class SlaAlertAction implements TriggerAction {

  public static final String type = "AlertAction";

  private static final Logger logger = Logger.getLogger(SlaAlertAction.class);

  private final String actionId;
  private final SlaOption slaOption;
  private final int execId;
  private final AlerterHolder alerters;
  private final ExecutorLoader executorLoader;

  //todo chengren311: move this class to executor module when all existing triggers in db are expired
  public SlaAlertAction(final String id, final SlaOption slaOption, final int execId) {
    this.actionId = id;
    this.slaOption = slaOption;
    this.execId = execId;
    this.alerters = ServiceProvider.SERVICE_PROVIDER.getInstance(AlerterHolder.class);
    this.executorLoader = ServiceProvider.SERVICE_PROVIDER.getInstance(ExecutorLoader.class);
  }

  public static SlaAlertAction createFromJson(final Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  public static SlaAlertAction createFromJson(final HashMap<String, Object> obj)
      throws Exception {
    final Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create action of " + type + " from "
          + jsonObj.get("type"));
    }
    final String actionId = (String) jsonObj.get("actionId");

    SlaOption slaOption;
    List<String> emails;
    // TODO edlu: is this being written? Handle both old and new formats, when written in new
    // format
     slaOption = SlaOption.fromObject(jsonObj.get("slaOption"));
    final int execId = Integer.valueOf((String) jsonObj.get("execId"));

    return new SlaAlertAction(actionId, slaOption, execId);
  }

  @Override
  public String getId() {
    return this.actionId;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public TriggerAction fromJson(final Object obj) throws Exception {
    return createFromJson(obj);
  }

  @Override
  public Object toJson() {
    final Map<String, Object> jsonObj = new HashMap<>();
    jsonObj.put("actionId", this.actionId);
    jsonObj.put("type", type);
    // TODO edlu: keeping the old format for now, upgrade to new format.
    jsonObj.put("slaAction", this.slaOption.toObject());
    jsonObj.put("execId", String.valueOf(this.execId));

    return jsonObj;
  }

  @Override
  public void doAction() throws Exception {
    logger.info("Alerting on sla failure.");
    if (slaOption.hasAlert()) {
      final ExecutableFlow flow = this.executorLoader.fetchExecutableFlow(this.execId);
      this.alerters.forEach((String alerterName, Alerter alerter) -> {
        try {
          alerter.alertOnSla(flow, this.slaOption);
        } catch (final Exception e) {
          logger.error("Failed to alert on SLA breach for execution " + flow.getExecutionId(), e);
        }
      });
    }
  }

  @Override
  public void setContext(final Map<String, Object> context) {
  }

  @Override
  public String getDescription() {
    return type + " for " + this.execId + " with " + this.slaOption.toString();
  }

}
