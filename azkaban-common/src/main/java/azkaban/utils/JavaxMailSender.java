package azkaban.utils;

import com.sun.mail.smtp.SMTPTransport;
import java.util.Properties;
import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.NoSuchProviderException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;

/**
 * Wraps javax.mail features, mostly because Session is a final class and can't be mocked.
 */
public class JavaxMailSender {

  private final Session session;
  private final SMTPTransport t;

  public JavaxMailSender(final Properties props, String protocol)
          throws NoSuchProviderException {
    this.session = Session.getInstance(props, null);
    this.t = (SMTPTransport) this.session.getTransport(protocol);
  }

  public Message createMessage() {
    return new MimeMessage(this.session);
  }

  public void connect(final String mailHost, final int mailPort, final String mailUser,
                      final String mailPassword) throws MessagingException {
    this.t.connect(mailHost, mailPort, mailUser, mailPassword);
  }

  public void connect() throws MessagingException {
    this.t.connect();
  }

  public void sendMessage(final Message message, final Address[] recipients)
          throws MessagingException {
    this.t.sendMessage(message, recipients);
  }

  public void close() throws MessagingException {
    this.t.close();
  }

}
