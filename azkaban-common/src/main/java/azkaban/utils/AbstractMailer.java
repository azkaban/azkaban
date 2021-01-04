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

  private static final int MB_IN_BYTES = 1048576;
  protected final EmailMessageCreator messageCreator;
  private final String azkabanName;

  private final long attachmentMazSizeInByte;

  public AbstractMailer(final Props props, final EmailMessageCreator messageCreator) {
    this.azkabanName = props.getString("azkaban.name", "azkaban");
    this.messageCreator = messageCreator;
    final long maxAttachmentSizeInMB =
        props.getInt("mail.max.attachment.size.mb", 100);
    this.attachmentMazSizeInByte = maxAttachmentSizeInMB * MB_IN_BYTES;
  }

  public EmailMessage createEmailMessage(final String subject, final String mimetype,
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

  /**
   * Attachment maximum size in bytes
   */
  long getAttachmentMaxSize() {
    return this.attachmentMazSizeInByte;
  }
}
