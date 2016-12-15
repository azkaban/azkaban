package azkaban.utils;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

/**
 * When we use the log4j Kafka appender, it seems that the appender simply does not log the stack trace anywhere
 * Seeing as the stack trace is a very important piece of information, we create our own PatternLayout class that
 * appends the stack trace to the log message that reported it, so that all the information regarding that error
 * can be found one in place.
 */
public class PatternLayoutEscaped extends PatternLayout {
  public PatternLayoutEscaped(String s) {
    super(s);
  }

  public PatternLayoutEscaped() {
    super();
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
