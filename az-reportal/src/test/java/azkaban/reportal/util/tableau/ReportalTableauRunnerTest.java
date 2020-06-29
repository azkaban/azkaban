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

package azkaban.reportal.util.tableau;

import static azkaban.jobtype.ReportalTableauRunner.TABLEAU_URL;
import static azkaban.jobtype.ReportalTableauRunner.TIMEOUT;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import azkaban.jobtype.ReportalTableauRunner;
import azkaban.utils.UndefinedPropertyException;
import java.util.Properties;
import org.junit.Test;

public class ReportalTableauRunnerTest {

  @Test
  public void testNoTimeout() throws InterruptedException {
    final Properties props = new Properties();
    props.put(TABLEAU_URL, "test");
    assertThatThrownBy(() -> {
      new ReportalTableauRunner("tableau", props);
    }).isInstanceOf
        (UndefinedPropertyException.class);
  }

  @Test
  public void testNoTableauUrl() throws InterruptedException {
    final Properties props = new Properties();
    props.put(TIMEOUT, "1");
    assertThatThrownBy(() -> {
      new ReportalTableauRunner("tableau", props);
    }).isInstanceOf
        (UndefinedPropertyException.class);
  }
}
