package azkaban.utils;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

/**
 * When we use log4j to send a JSON message the official PatternLayout will send corrupt JSON if certain characters are
 * present. We use this PatternLayoutEscaped class as a thin interface around PatternLayout to escape all these
 * characters.
 */
public class PatternLayoutEscaped extends PatternLayout {
  public String format(final LoggingEvent event) {
    if (event.getMessage() instanceof String) {
      return super.format(copyAndEscapeEvent(event));
    }
    return super.format(event);
  }

  /**
   * Create a copy of event, but escape backslashes, tabs, newlines and quotes in its message
   */
  private LoggingEvent copyAndEscapeEvent(LoggingEvent event) {
    String message = event.getMessage().toString();
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
