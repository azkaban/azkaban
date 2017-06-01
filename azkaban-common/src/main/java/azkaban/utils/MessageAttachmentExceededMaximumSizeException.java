package azkaban.utils;

import javax.mail.MessagingException;

/**
 * To indicate the attachment size is larger than allowed size
 *
 * @author hluu
 */
public class MessageAttachmentExceededMaximumSizeException extends
    MessagingException {

  public MessageAttachmentExceededMaximumSizeException() {
    super();
  }

  public MessageAttachmentExceededMaximumSizeException(final String s) {
    super(s);
  }

  public MessageAttachmentExceededMaximumSizeException(final String s, final Exception e) {
    super(s, e);
  }

}
