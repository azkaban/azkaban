/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.jobtype.tuning;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;


/**
 * This class is responsible for finding whether failure is because of tuning parameters.
 * This try to search predefined patterns in the log.
 */
public class TuningErrorDetector {
  private Logger log = Logger.getRootLogger();

  private static List<Pattern> errorPatterns = new ArrayList<>();

  static {
    Pattern pattern1 = Pattern.compile(".*Error: Java heap space.*");
    Pattern pattern2 = Pattern.compile(".*java.lang.OutOfMemoryError.*");
    Pattern pattern3 = Pattern.compile(".*Container .* is running beyond virtual memory limits.*");
    Pattern pattern4 = Pattern.compile(".*Initialization of all the collectors failed.*");
    errorPatterns.add(pattern1);
    errorPatterns.add(pattern2);
    errorPatterns.add(pattern3);
    errorPatterns.add(pattern4);
  }

  public boolean containsAutoTuningError(String logMessage) {
    if (logMessage != null) {
      for (Pattern pattern : errorPatterns) {
        Matcher matcher = pattern.matcher(logMessage);
        if (matcher.matches()) {
          log.error("Categorizing this error as autotuning error because of this message: " + logMessage);
          return true;
        }
      }
    }
    return false;
  }
}
