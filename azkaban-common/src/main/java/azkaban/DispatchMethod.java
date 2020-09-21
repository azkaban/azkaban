package azkaban;

import org.apache.log4j.Logger;

public enum DispatchMethod {
  PUSH,
  POLL,
  PUSH_CONTAINERIZED;
  private static final Logger logger = Logger.getLogger(DispatchMethod.class);

  public static DispatchMethod getDispatchMethod(String value) {
    try {
      logger.info("Value of dispatch method is : " + value);
      return DispatchMethod.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException iae) {
      logger.info("Incorrect value is set for dispatch method. The default dispatch method, PUSH,"
          + " is used");
      return DispatchMethod.PUSH;
    }
  }

  public static boolean isPollMethodEnabled(String dispatchMethod) {
    return DispatchMethod.getDispatchMethod(dispatchMethod) == DispatchMethod.POLL;
  }

  public static boolean isPushMethodEnabled(String dispatchMethod) {
    return DispatchMethod.getDispatchMethod(dispatchMethod) == DispatchMethod.PUSH;
  }

  public static boolean isPushContainerizedMethodEnabled(String dispatchMethod) {
    return DispatchMethod.getDispatchMethod(dispatchMethod) == DispatchMethod.PUSH_CONTAINERIZED;
  }
}
