package azkaban.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.helpers.PatternParser;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;


/**
 * When we use the log4j Kafka appender, it seems that the appender simply does not log the stack trace anywhere
 * Seeing as the stack trace is a very important piece of information, we create our own PatternLayout class that
 * appends the stack trace to the log message that reported it, so that all the information regarding that error
 * can be found one in place. In addition, we create a new PatternParser to deliver logs with current host name.
 */
public class AzkabanPatternLayout extends PatternLayout {
  protected String host;

  public AzkabanPatternLayout(String s) {
    super(s);
  }

  public AzkabanPatternLayout() {
    super();
  }

  // TODO: There is no easy way to get Jetty Server's port by InetAddress API.
  // We might need to find some other way to find this application's port
  protected String getPort() {
    return "";
  }

  protected String getHostname() {
    if (host == null) {
      try {
        InetAddress addr = InetAddress.getLocalHost();
        this.host = addr.getHostName();
      } catch (UnknownHostException e) {

        // Prevent exposing exceptions when something goes wrong.
        this.host = "localhost";
      }
    }
    return host;
  }

  @Override
  protected PatternParser createPatternParser(String pattern) {
    return new PatternParser(pattern) {

      @Override
      protected void finalizeConverter(char c) {
        PatternConverter pc = null;

        switch (c) {
          case 'h':
            pc = new PatternConverter() {
              @Override
              protected String convert(LoggingEvent event) {
                return getHostname();
              }
            };
            break;
        }
        if (pc==null)
          super.finalizeConverter(c);
        else
          addConverter(pc);
      }
    };
  }

  @Override
  public String format(final LoggingEvent event) {
    if (event.getMessage() instanceof String) {
      return super.format(appendStackTraceToEvent(event));
    }
    return super.format(event);
  }

  /**
   * Create a copy of event, but append a stack trace to the message (if it exists).
   * Then it escapes the backslashes, tabs, newlines and quotes in its message as we are sending it as JSON and we
   * don't want any corruption of the JSON object.
   */
  private LoggingEvent appendStackTraceToEvent(LoggingEvent event) {
    String message = event.getMessage().toString();
    // If there is a stack trace available, print it out
    if (event.getThrowableInformation() != null) {
      String[] s = event.getThrowableStrRep();
      for (String line: s) {
        message += "\n" + line;
      }
    }
    message = message
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace("\"", "\\\"")
        .replace("\t", "\\t");

    Throwable throwable = event.getThrowableInformation() == null ? null
        : event.getThrowableInformation().getThrowable();
    return new LoggingEvent(event.getFQNOfLoggerClass(),
        event.getLogger(),
        event.getTimeStamp(),
        event.getLevel(),
        message,
        throwable);
  }
}
