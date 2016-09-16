/*
 * Copyright 2016 LinkedIn Corp.
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

package azkaban.restli;

import static org.junit.Assert.assertEquals;

import azkaban.project.validator.ValidationReport;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/**
 * Test response to deploy with either warn or error reports.
 * If a report has any errors, a RestLiServiceException should be thrown.
 * Warnings should not elicit an exception.
 * If an exception does get raised, it should carry an HTTP 400 status
 */

public class DeployProjectTest {
  @Test
  public void testWarnedDeploy() {
    ProjectManagerResource resource = new ProjectManagerResource();
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();
    for (int i = 0; i < 3; i++) {
      addMockWarning(reports, "Test warn level info message.");
    }
    // We expect that no exceptions are thrown given a
    // report with warnings. Anything thrown will result in failure
    resource.checkReports(reports);
  }

  @Test
  public void testErrorDeploy() {
    ProjectManagerResource resource = new ProjectManagerResource();
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();
    for (int i = 0; i < 3; i++) {
      addMockError(reports, "Test error level info message.");
    }
    // We expect that a RestLiServiceException is thrown given a
    // report with errors. Uncaught exceptions will result in failure
    try {
      resource.checkReports(reports);
    } catch (RestLiServiceException e) {
      //Ensure we have the right status code and exit
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
    }
  }

  @Test
  public void testWarnErrorDeploy() {
    ProjectManagerResource resource = new ProjectManagerResource();
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();
    for (int i = 0; i < 7; i++) {
      // If i is even, make an error report, otherwise make a warning report
      if (i % 2 == 0) {
        addMockError(reports, "Test error level info message.");
      }
      else {
        addMockWarning(reports, "Test warn level info message.");
      }
    }

    // We expect that a RestLiServiceException is thrown given a
    // report with errors. Uncaught exceptions will result in failure
    try {
      resource.checkReports(reports);
    } catch (RestLiServiceException e) {
      //Ensure we have the right status code and exit
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
    }
  }

  /**
   * Test that an error message is attached to the exception on an error
   */
  @Test
  public void testErrorMessageDeploy() {
    ProjectManagerResource resource = new ProjectManagerResource();
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();

    addMockError(reports, "This should show up.");

    // We expect that a RestLiServiceException is thrown given a
    // report with errors. Uncaught exceptions will result in failure
    try {
      resource.checkReports(reports);
    } catch (RestLiServiceException e) {
      //Ensure we have the right status code and exit
      assertEquals(e.getStatus(), HttpStatus.S_400_BAD_REQUEST);
      assertEquals(e.getMessage(), "Validator Error reports errors: This should show up."
          + System.getProperty("line.separator"));
    }
  }

  private void addMockWarning(Map<String, ValidationReport> reports, String msg) {
    Set<String> warnMsgs = new HashSet<String>();
    ValidationReport warnRpt = new ValidationReport();
    warnMsgs.add(msg);
    warnRpt.addWarningMsgs(warnMsgs);
    reports.put("Warn", warnRpt);
  }

  private void addMockError(Map<String, ValidationReport> reports, String msg) {
    Set<String> errorMsgs = new HashSet<String>();
    ValidationReport errorRpt = new ValidationReport();
    errorMsgs.add(msg);
    errorRpt.addErrorMsgs(errorMsgs);
    reports.put("Error", errorRpt);
  }
}
