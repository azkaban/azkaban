/*
 * Copyright 2014 LinkedIn Corp.
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

package azkaban.utils;

import java.util.Calendar;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for azkaban.utils.Utils
 */
public class UtilsTest {

  /* Test negative port case */
  @Test
  public void testNegativePort() {
    Assert.assertFalse(Utils.isValidPort(-1));
    Assert.assertFalse(Utils.isValidPort(-10));
  }

  /* Test zero port case */
  @Test
  public void testZeroPort() {
    Assert.assertFalse(Utils.isValidPort(0));
  }

  /* Test port beyond limit */
  @Test
  public void testOverflowPort() {
    Assert.assertFalse(Utils.isValidPort(70000));
    Assert.assertFalse(Utils.isValidPort(65536));
  }

  /* Test happy isValidPort case*/
  @Test
  public void testValidPort() {
    Assert.assertTrue(Utils.isValidPort(1023));
    Assert.assertTrue(Utils.isValidPort(10000));
    Assert.assertTrue(Utils.isValidPort(3030));
    Assert.assertTrue(Utils.isValidPort(1045));
  }

  /* Test CronExpression valid cases*/
  @Test
  public void testValidCronExpressionV() {

    final DateTimeZone timezone = DateTimeZone.getDefault();
    final int year = Calendar.getInstance().get(Calendar.YEAR);
    Assert.assertTrue(Utils.isCronExpressionValid("0 0 3 ? * *", timezone));
    Assert.assertTrue(Utils.isCronExpressionValid("0 0 3 ? * * " + year, timezone));
    Assert.assertTrue(Utils.isCronExpressionValid("0 0 * ? * *", timezone));
    Assert.assertTrue(Utils.isCronExpressionValid("0 0 * ? * FRI", timezone));

    // This is a bug from Quartz Cron. It looks like Quartz will parse the preceding 7 fields of a String.
    Assert.assertTrue(Utils.isCronExpressionValid("0 0 3 ? * * " + year + " 22", timezone));
  }

  /* Test CronExpression invalid cases*/
  @Test
  public void testInvalidCronExpression() {

    final DateTimeZone timezone = DateTimeZone.getDefault();
    Assert.assertFalse(Utils.isCronExpressionValid("0 0 3 * * *", timezone));
    Assert.assertFalse(Utils.isCronExpressionValid("0 66 * ? * *", timezone));
    Assert.assertFalse(Utils.isCronExpressionValid("0 * * ? * 8", timezone));
    Assert.assertFalse(Utils.isCronExpressionValid("0 * 25 ? * FRI", timezone));
  }
}
