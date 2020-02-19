/*
 * Copyright 2019 LinkedIn Corp.
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

package azkaban.scheduler;

import static azkaban.test.executions.ThinArchiveTestUtils.depSetEq;
import static azkaban.utils.ThinArchiveUtils.getStartupDependenciesFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import azkaban.project.ArchiveUnthinner;
import azkaban.project.JdbcDependencyManager;
import azkaban.project.Project;
import azkaban.project.ProjectManagerException;
import azkaban.project.validator.ValidationReport;
import azkaban.project.validator.ValidationStatus;
import azkaban.spi.Dependency;
import azkaban.spi.DependencyFile;
import azkaban.spi.FileValidationStatus;
import azkaban.test.executions.ThinArchiveTestUtils;
import azkaban.utils.DependencyTransferException;
import azkaban.utils.DependencyTransferManager;
import azkaban.utils.ValidatorUtils;
import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.stubbing.Answer;
import org.skyscreamer.jsonassert.JSONAssert;

public class CronCalculatorTest {

  @Test
  public void testCronPatterns() {
    CronCalculator cronA = new CronCalculator("0 0 0 0 0 * 2020");
    assertTrue(cronA.isStatic());
    assertTrue(!cronA.isUnbounded());
    CronCalculator cronB = new CronCalculator("0 0 0 0 0 * *");
    assertTrue(!cronB.isStatic());
    assertTrue(cronB.isUnbounded());
    CronCalculator cronC = new CronCalculator("0 0 0 0 * * 2020");
    assertTrue(!cronC.isStatic());
    assertTrue(!cronC.isUnbounded());
    CronCalculator cronD = new CronCalculator("0 0 5-10 1 1,5 * 2020");
    assertTrue(!cronD.isStatic());
    assertTrue(!cronD.isUnbounded());
    assertEquals(getDate(2020, 5, 1, 10, 0, 0), cronD.getUpperBound());
    CronCalculator cronE = new CronCalculator("0 0 5-10 1 1,5 *");
    assertTrue(!cronE.isStatic());
    assertTrue(cronE.isUnbounded());
    CronCalculator cronF = new CronCalculator("0 5-10 1 1,5 *");
    assertTrue(!cronF.isStatic());
    assertTrue(cronF.isUnbounded());
  }

  private Date getDate(int year, int month, int day, int hour, int min, int sec) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(0);
    calendar.set(year, month, day, hour, min, sec);
    return calendar.getTime();
  }
}
