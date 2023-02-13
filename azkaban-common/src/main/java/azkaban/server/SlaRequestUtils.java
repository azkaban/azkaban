/*
 * Copyright 2020 LinkedIn Corp.
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

import static azkaban.server.HttpRequestUtils.getMapParamGroup;
import static azkaban.server.HttpRequestUtils.getParam;
import static azkaban.server.HttpRequestUtils.getParamGroup;

import azkaban.sla.SlaAction;
import azkaban.sla.SlaOption;
import azkaban.sla.SlaOption.SlaOptionBuilder;
import azkaban.sla.SlaType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.apache.log4j.Logger;


public class SlaRequestUtils {

  public static final String PARAM_SLA_EMAILS = "slaEmails";
  public static final String PARAM_SLA_ALERTERS = "slaAlerters";
  public static final String SLA_STATUS_SUCCESS = "SUCCESS";
  private static final Logger logger = Logger.getLogger(SlaRequestUtils.class);

  public static List<SlaOption> parseSlaOptions(final HttpServletRequest req, final String flowName,
      final String settingsParamName) throws ServletException {
    final Map<String, String> settings = getParamGroup(req, settingsParamName);
    final Map<String, Map<String, String>> alertersConfigs = getMapParamGroup(req, PARAM_SLA_ALERTERS);
    String slaEmailsStr = getParam(req, PARAM_SLA_EMAILS, null);

    // Don't allow combining old & new in the same request:
    // This ensures that there's no need to handle possible conflicts between 'slaEmails' and
    // 'slaAlerters[email][recipients]'.
    if (slaEmailsStr != null && !alertersConfigs.isEmpty()) {
      throw new ServletException("The legacy slaEmails param is not allowed in combination with "
          + "the slaAlerters param group. Please, set 'slaAlerters[email][recipients]' instead of "
          + "'slaEmails'.");
    }

    // TODO ypadron: migrate legacy email alert handling to the alerter plugins approach.
    //  Treat email is a built-in alerter.
    final Map<String, String> emailAlerterConfigs = alertersConfigs.remove("email");
    if (emailAlerterConfigs != null) {
      slaEmailsStr = emailAlerterConfigs.get("recipients");
    }

    final List<String> emailAlerterRecipients;
    if (slaEmailsStr == null) {
      emailAlerterRecipients = Arrays.asList();
    } else {
      final String[] emailSplit = slaEmailsStr.split("\\s*,\\s*|\\s*;\\s*|\\s+");
      emailAlerterRecipients = Arrays.asList(emailSplit);
    }

    final List<SlaOption> slaOptions = new ArrayList<>();
    for (final String set : settings.keySet()) {
      final SlaOption slaOption;
      try {
        slaOption = parseSlaSetting(settings.get(set), flowName, emailAlerterRecipients, alertersConfigs);
      } catch (final Exception e) {
        throw new ServletException(
            "Error parsing SLA setting '" + settings.get(set) + "': " + e.toString(), e);
      }
      slaOptions.add(slaOption);
    }
    return slaOptions;
  }

  private static SlaOption parseSlaSetting(final String set, final String flowName, final List<String> emails,
      final Map<String, Map<String, String>> alertersConfigs) throws ServletException {
    logger.info("Trying to parse sla with the following set: " + set);

    final String[] parts = set.split(",", -1);
    final String id = parts[0];
    final String rule = parts[1];
    final String duration = parts[2];
    final String emailAction = parts[3];
    final String killAction = parts[4];

    final SlaType type;
    if (id.length() == 0) {
      if (rule.equals(SLA_STATUS_SUCCESS)) {
        type = SlaType.FLOW_SUCCEED;
      } else {
        type = SlaType.FLOW_FINISH;
      }
    } else { // JOB
      if (rule.equals(SLA_STATUS_SUCCESS)) {
        type = SlaType.JOB_SUCCEED;
      } else {
        type = SlaType.JOB_FINISH;
      }
    }
    final HashSet<SlaAction> actions = new HashSet<>();
    if (emailAction.equals("true")) {
      actions.add(SlaAction.ALERT);
    }
    if (killAction.equals("true")) {
      actions.add(SlaAction.KILL);
    }

    final Duration dur;
    try {
      dur = parseDuration(duration);
    } catch (final Exception e) {
      throw new ServletException(
          "Unable to parse duration for a SLA that needs to take actions!", e);
    }

    if (actions.isEmpty()) {
      throw new ServletException("Unable to create SLA as there is no action set");
    }
    logger.info("Parsing sla as id:" + id + " type:" + type + " sla:"
        + rule + " Duration:" + duration + " actions:" + actions);
    return new SlaOptionBuilder(type, flowName, dur).setJobName(id).setActions(actions)
        .setEmails(emails).setAlertersConfigs(alertersConfigs).createSlaOption();
  }

  private static Duration parseDuration(final String duration) {
    final int hour = Integer.parseInt(duration.split(":")[0]);
    final int min = Integer.parseInt(duration.split(":")[1]);
    return Duration.ofMinutes(min + hour * 60);
  }

}
