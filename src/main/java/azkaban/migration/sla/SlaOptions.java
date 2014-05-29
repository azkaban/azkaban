/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.migration.sla;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import azkaban.migration.sla.SLA.SlaSetting;

@Deprecated
public class SlaOptions {

  public List<String> getSlaEmails() {
    return slaEmails;
  }

  public void setSlaEmails(List<String> slaEmails) {
    this.slaEmails = slaEmails;
  }

  public List<SlaSetting> getSettings() {
    return settings;
  }

  public void setSettings(List<SlaSetting> settings) {
    this.settings = settings;
  }

  private List<String> slaEmails;
  private List<SlaSetting> settings;

  public Object toObject() {
    Map<String, Object> obj = new HashMap<String, Object>();
    obj.put("slaEmails", slaEmails);
    List<Object> slaSettings = new ArrayList<Object>();
    for (SlaSetting s : settings) {
      slaSettings.add(s.toObject());
    }
    obj.put("settings", slaSettings);
    return obj;
  }

  @SuppressWarnings("unchecked")
  public static SlaOptions fromObject(Object object) {
    if (object != null) {
      SlaOptions slaOptions = new SlaOptions();
      Map<String, Object> obj = (HashMap<String, Object>) object;
      slaOptions.setSlaEmails((List<String>) obj.get("slaEmails"));
      List<SlaSetting> slaSets = new ArrayList<SlaSetting>();
      for (Object set : (List<Object>) obj.get("settings")) {
        slaSets.add(SlaSetting.fromObject(set));
      }
      slaOptions.setSettings(slaSets);
      return slaOptions;
    }
    return null;
  }
}
