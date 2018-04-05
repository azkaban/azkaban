package azkaban.jobtype.javautils;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class JobUtils {

  public static Logger initJobLogger() {
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.removeAllAppenders();
    ConsoleAppender appender = new ConsoleAppender(new PatternLayout("%p %m\n"));
    appender.activateOptions();
    rootLogger.addAppender(appender);
    return rootLogger;
  }
}
