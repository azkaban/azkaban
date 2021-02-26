/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This enum contains list of dispatch types implemented in Azkaban.
 */
public enum DispatchMethod {
  PUSH(0),
  POLL(1),
  CONTAINERIZED(2);
  private static final Logger logger = LoggerFactory.getLogger(DispatchMethod.class);
  private final int numVal;

  DispatchMethod(final int numVal) {
    this.numVal = numVal;
  }

  public int getNumVal() {
    return this.numVal;
  }

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

  public static boolean isContainerizedMethodEnabled(String dispatchMethod) {
    return DispatchMethod.getDispatchMethod(dispatchMethod) == DispatchMethod.CONTAINERIZED;
  }
}
