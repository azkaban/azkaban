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
import org.apache.log4j.Logger;

public class SlaRequestUtils {

  public static final String SLA_STATUS_SUCCESS = "SUCCESS";
  private static final Logger logger = Logger.getLogger(SlaRequestUtils.class);

  public static List<SlaOption> parseSlaOptions(final String flowName, final String emailStr,
      final Map<String, String> settings) throws ServletException {
    final List<String> slaEmails;
    if (emailStr == null) {
      slaEmails = Arrays.asList();
    } else {
      final String[] emailSplit = emailStr.split("\\s*,\\s*|\\s*;\\s*|\\s+");
      slaEmails = Arrays.asList(emailSplit);
    }

    final List<SlaOption> slaOptions = new ArrayList<>();
    for (final String set : settings.keySet()) {
      final SlaOption slaOption;
      try {
        slaOption = parseSlaSetting(settings.get(set), flowName, slaEmails);
      } catch (final Exception e) {
        throw new ServletException(e);
      }
      slaOptions.add(slaOption);
    }
    return slaOptions;
  }

  private static SlaOption parseSlaSetting(final String set, final String flowName,
      final List<String> emails) throws ServletException {
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
        .setEmails(emails).createSlaOption();
  }

  private static Duration parseDuration(final String duration) {
    final int hour = Integer.parseInt(duration.split(":")[0]);
    final int min = Integer.parseInt(duration.split(":")[1]);
    return Duration.ofMinutes(min + hour * 60);
  }

}
