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

package azkaban.webapp.servlet;

import static org.junit.Assert.assertEquals;

import azkaban.project.ProjectManagerException;
import azkaban.project.validator.ValidationReport;
import azkaban.restli.ProjectManagerResource;

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
  public void testWarnedDeploy() throws Exception {
    ProjectManagerResource resource = new ProjectManagerResource();
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();
    for (int i = 0; i < 3; i++) {
      Set<String> warnMsgs = new HashSet<String>();
      ValidationReport warnRpt = new ValidationReport();
      warnMsgs.add("test warn level info message.");
      warnRpt.addWarningMsgs(warnMsgs);
      reports.put("warn " + i, warnRpt);
    }
    // We expect that no exceptions are thrown given a
    // report with warnings
    boolean pass = true;
    try {
      resource.checkReport(reports);
    } catch (RestLiServiceException e) {
      pass = false;
    } catch (Exception e) {
      pass = false;
    }
    assertEquals(true, pass);
  }

  @Test
  public void testErrorDeploy() throws Exception {
    ProjectManagerResource resource = new ProjectManagerResource();
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();
    for (int i = 0; i < 3; i++) {
      Set<String> errorMsgs = new HashSet<String>();
      ValidationReport errorRpt = new ValidationReport();
      errorMsgs.add("test error level info message.");
      errorRpt.addErrorMsgs(errorMsgs);
      reports.put("error " + i, errorRpt);
    }
    // We expect that a ProjectManagerException is thrown given a
    // report with errors
    boolean pass = false;
    try {
      resource.checkReport(reports);
    } catch (RestLiServiceException e) {
      if (e.getStatus() != HttpStatus.S_400_BAD_REQUEST) {
        pass = false;
      } else {
        pass = true;
      }
    } catch (Exception e) {
      pass = false;
    }
    assertEquals(true, pass);
  }

  @Test
  public void testWarnErrorDeploy() throws Exception {
    ProjectManagerResource resource = new ProjectManagerResource();
    Map<String, ValidationReport> reports = new LinkedHashMap<String, ValidationReport>();

    Set<String> errorMsgs = new HashSet<String>();
    Set<String> warnMsgs = new HashSet<String>();
    ValidationReport errorRpt = new ValidationReport();
    ValidationReport warnRpt = new ValidationReport();
    errorMsgs.add("test error level info message.");
    errorRpt.addErrorMsgs(errorMsgs);
    reports.put("error", errorRpt);
    warnMsgs.add("test warn level info message.");
    warnRpt.addWarningMsgs(warnMsgs);
    reports.put("warn", warnRpt);

    // We expect that a ProjectManagerException is thrown given a
    // report with errors
    boolean pass = false;
    try {
      resource.checkReport(reports);
    } catch (RestLiServiceException e) {
      if (e.getStatus() != HttpStatus.S_400_BAD_REQUEST) {
        pass = false;
      } else {
        pass = true;
      }
    } catch (Exception e) {
      pass = false;
    }
    assertEquals(true, pass);
  }
}