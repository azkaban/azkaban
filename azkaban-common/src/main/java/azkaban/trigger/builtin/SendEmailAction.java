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
import java.util.List;
import java.util.Map;

import azkaban.trigger.TriggerAction;
import azkaban.utils.AbstractMailer;
import azkaban.utils.EmailMessage;
import azkaban.utils.Props;

public class SendEmailAction implements TriggerAction {

  private String actionId;
  private static AbstractMailer mailer;
  private String message;
  public static final String type = "SendEmailAction";
  private String mimetype = "text/html";
  private List<String> emailList;
  private String subject;

  public static void init(Props props) {
    mailer = new AbstractMailer(props);
  }

  public SendEmailAction(String actionId, String subject, String message,
      List<String> emailList) {
    this.actionId = actionId;
    this.message = message;
    this.subject = subject;
    this.emailList = emailList;
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
  public static SendEmailAction createFromJson(Object obj) throws Exception {
    Map<String, Object> jsonObj = (HashMap<String, Object>) obj;
    if (!jsonObj.get("type").equals(type)) {
      throw new Exception("Cannot create action of " + type + " from "
          + jsonObj.get("type"));
    }
    String actionId = (String) jsonObj.get("actionId");
    String subject = (String) jsonObj.get("subject");
    String message = (String) jsonObj.get("message");
    List<String> emailList = (List<String>) jsonObj.get("emailList");
    return new SendEmailAction(actionId, subject, message, emailList);
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
    jsonObj.put("subject", subject);
    jsonObj.put("message", message);
    jsonObj.put("emailList", emailList);

    return jsonObj;
  }

  @Override
  public void doAction() throws Exception {
    EmailMessage email =
        mailer.prepareEmailMessage(subject, mimetype, emailList);
    email.setBody(message);
    email.sendEmail();
  }

  @Override
  public void setContext(Map<String, Object> context) {

  }

  @Override
  public String getDescription() {
    return type;
  }

}
