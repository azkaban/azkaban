package azkaban.utils;

import java.util.Properties;
import javax.inject.Inject;
import javax.mail.NoSuchProviderException;

public class EmailMessageCreator {

  public static final int DEFAULT_SMTP_PORT = 25;

  private final String mailHost;
  private final int mailPort;
  private final String mailUser;
  private final String mailPassword;
  private final String mailSender;
  private final String tls;
  private final boolean usesAuth;

  @Inject
  public EmailMessageCreator(final Props props) {
    this.mailHost = props.getString("mail.host", "localhost");
    this.mailPort = props.getInt("mail.port", DEFAULT_SMTP_PORT);
    this.mailUser = props.getString("mail.user", "");
    this.mailPassword = props.getString("mail.password", "");
    this.mailSender = props.getString("mail.sender", "");
    this.tls = props.getString("mail.tls", "false");
    this.usesAuth = props.getBoolean("mail.useAuth", true);
  }

  public EmailMessage createMessage() {
    final EmailMessage message = new EmailMessage(
        this.mailHost, this.mailPort, this.mailUser, this.mailPassword, this);
    message.setFromAddress(this.mailSender);
    message.setTLS(this.tls);
    message.setAuth(this.usesAuth);
    return message;
  }

  public JavaxMailSender createSender(final Properties props) throws NoSuchProviderException {
    return new JavaxMailSender(props);
  }
}
