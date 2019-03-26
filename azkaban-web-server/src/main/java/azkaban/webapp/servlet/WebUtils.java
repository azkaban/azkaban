/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.webapp.servlet;

import azkaban.executor.Status;

public class WebUtils {

  public static String formatStatus(final Status status) {
    switch (status) {
      case SUCCEEDED:
        return "Success";
      case FAILED:
        return "Failed";
      case RUNNING:
        return "Running";
      case DISABLED:
        return "Disabled";
      case KILLED:
        return "Killed";
      case FAILED_FINISHING:
        return "Running w/Failure";
      case PREPARING:
        return "Preparing";
      case READY:
        return "Ready";
      case PAUSED:
        return "Paused";
      case SKIPPED:
        return "Skipped";
      case KILLING:
        return "Killing";
      default:
    }
    return "Unknown";
  }

}
