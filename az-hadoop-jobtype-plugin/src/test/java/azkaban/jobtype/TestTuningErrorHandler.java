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

package azkaban.jobtype;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import azkaban.jobtype.tuning.TuningErrorDetector;


public class TestTuningErrorHandler {

  @Test
  public void testTuningErrorHandler() throws IOException {
    TuningErrorDetector tuningErrorDetector = new TuningErrorDetector();
    String message = "Error: Java heap space. Got following exception ";
    Assert.assertTrue("TuningErrorHandler not identifying error pattern ",
        tuningErrorDetector.containsAutoTuningError(message));

    message = "Error: java.lang.OutOfMemoryError: unable to create new native thread by server ";
    Assert.assertTrue("TuningErrorHandler not identifying error pattern ",
        tuningErrorDetector.containsAutoTuningError(message));

    message = "Initialization of all the collectors failed on cluster";
    Assert.assertTrue("TuningErrorHandler not identifying error pattern ",
        tuningErrorDetector.containsAutoTuningError(message));

    message = "Container 10002 is running beyond virtual memory limits";
    Assert.assertTrue("TuningErrorHandler not identifying error pattern ",
        tuningErrorDetector.containsAutoTuningError(message));

    message = "Failed to find class";
    Assert.assertFalse("TuningErrorHandler not identifying error pattern ",
        tuningErrorDetector.containsAutoTuningError(message));

  }

}
