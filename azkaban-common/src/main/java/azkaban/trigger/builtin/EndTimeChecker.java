/*
 * Copyright 2017 LinkedIn Corp.
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

import azkaban.trigger.ConditionChecker;
import java.util.HashMap;
import java.util.Map;


public class EndTimeChecker implements ConditionChecker {

  public static final String type = "EndTimeChecker";

  private long endCheckTime;
  private final String id;

  public EndTimeChecker(String id) {
    this(id, -1);
  }

  public EndTimeChecker(String id, long endCheckTime) {
    this.id = id;
    this.endCheckTime = endCheckTime;
  }

  public long getEndCheckTime() {
    return endCheckTime;
  }

  @Override
  public Boolean eval() {
    /*
     * Expire condition must satisfy two criteria:
     * 1). endCheckTime > 0 (the trigger has expire condition);
     * 2). current time passed endCheckTime.
     */
    return endCheckTime > 0 && endCheckTime < System.currentTimeMillis();
  }

  @Override
  public void reset() {
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getType() {
    return type;
  }

  @SuppressWarnings("unchecked")
  public static EndTimeChecker createFromJson(Object obj) throws Exception {
    return createFromJson((HashMap<String, Object>) obj);
  }

  public static EndTimeChecker createFromJson(HashMap<String, Object> obj)
      throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create checker of " + type + " from "
          + jsonObj.get("type"));
    }
    Long firstCheckTime = Long.valueOf((String) jsonObj.get("endCheckTime"));
    String id = (String) jsonObj.get("id");
    return new EndTimeChecker(id, firstCheckTime);
  }

  @Override
  public EndTimeChecker fromJson(Object obj) throws Exception {
    return createFromJson(obj);
  }

  @Override
  public Object getNum() {
    return null;
  }

  @Override
  public Object toJson() {
    Map<String, Object> jsonObj = new HashMap<String, Object>();
    jsonObj.put("type", type);
    jsonObj.put("endCheckTime", String.valueOf(endCheckTime));
    jsonObj.put("id", id);
    return jsonObj;
  }

  @Override
  public void stopChecker() {
    this.endCheckTime = -1;
  }

  @Override
  public void setContext(Map<String, Object> context) {
  }

  @Override
  public long getNextCheckTime() {
    return 0;
  }

}
