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

package azkaban.reportal;

import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import azkaban.jobtype.ReportalPrestoRunner;
import java.util.Properties;
import org.junit.Test;

public class ReportalPrestoRunnerTest {

  @Test
  public void testNoDriverURL() throws InterruptedException {
    final Properties props = new Properties();
    props.put(ReportalPrestoRunner.DRIVER_URL, "test");
    assertThatThrownBy(() -> {
      new ReportalPrestoRunner("presto", props);
    }).isInstanceOf
        (IllegalArgumentException.class);
  }

  @Test
  public void testNoPrestoUser() throws InterruptedException {
    final Properties props = new Properties();
    props.put(ReportalPrestoRunner.PRESTO_USER, "test");
    assertThatThrownBy(() -> {
      new ReportalPrestoRunner("presto", props);
    }).isInstanceOf
        (IllegalArgumentException.class);
  }

  @Test
  public void testNoJdbcDriverKey() throws InterruptedException {
    final Properties props = new Properties();
    props.put(ReportalPrestoRunner.JDBC_DRIVER_KEY, "test");
    assertThatThrownBy(() -> {
      new ReportalPrestoRunner("presto", props);
    }).isInstanceOf
        (IllegalArgumentException.class);
  }
}
