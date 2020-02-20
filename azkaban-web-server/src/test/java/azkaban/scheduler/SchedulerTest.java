/*
 * Copyright 2017 LinkedIn Corp.
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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import azkaban.webapp.servlet.ScheduleServlet;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class SchedulerTest {

  @Test
  public void testGetTimezone() throws Exception {
    assert null != ScheduleServlet.getTimeZone("UTC");
    assert null == ScheduleServlet.getTimeZone("FOO");
  }

}
