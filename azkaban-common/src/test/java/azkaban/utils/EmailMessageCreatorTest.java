package azkaban.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EmailMessageCreatorTest {

  private static final String HOST = "smtp.domain.com";
  private static final int MAIL_PORT = 25;
  private static final String SENDER = "somebody@domain.com";
  private static final String USER = "somebody@domain.com";
  private static final String PASSWORD = "pwd";

  @Test
  public void createMessage() {
    final Props props = new Props();
    props.put("mail.user", USER);
    props.put("mail.password", PASSWORD);
    props.put("mail.sender", SENDER);
    props.put("mail.host", HOST);
    props.put("mail.port", MAIL_PORT);
    final EmailMessageCreator creator = new EmailMessageCreator(props);
    final EmailMessage message = creator.createMessage();

    assertEquals(MAIL_PORT, message.getMailPort());
    assertEquals("", message.getBody());
    assertEquals(null, message.getSubject());
  }

}
