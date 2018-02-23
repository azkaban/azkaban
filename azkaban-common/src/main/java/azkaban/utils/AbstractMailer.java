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

import java.util.Collection;

public class AbstractMailer {

  protected final EmailMessageCreator messageCreator;
  private final String azkabanName;

  public AbstractMailer(final Props props, final EmailMessageCreator messageCreator) {
    this.azkabanName = props.getString("azkaban.name", "azkaban");
    this.messageCreator = messageCreator;
  }

  protected EmailMessage createEmailMessage(final String subject, final String mimetype,
      final Collection<String> emailList) {
    final EmailMessage message = this.messageCreator.createMessage();
    message.addAllToAddress(emailList);
    message.setMimeType(mimetype);
    message.setSubject(subject);
    return message;
  }

  public String getAzkabanName() {
    return this.azkabanName;
  }

}
