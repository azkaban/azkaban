/*
 * Copyright 2020 LinkedIn Corp.
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
package azkaban.executor.container;

import static azkaban.Constants.ContainerizedDispatchManagerProperties.KUBERNETES_VPA_FLOW_FILTER_FILE;
import azkaban.utils.Props;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VPAFlowCriteriaTest {

  private VPAFlowCriteria vpaFlowCriteria;

  private static final Logger logger = LoggerFactory.getLogger(VPAFlowCriteriaTest.class);

  @Before
  public void setup() throws Exception {
    final Props props = new Props();
    props.put(KUBERNETES_VPA_FLOW_FILTER_FILE, "src/test/resources/flow_filter.txt");
    this.vpaFlowCriteria = new VPAFlowCriteria(props, logger);
  }

  @Test
  public void testProjectNameFilter() throws Exception {
    Assert.assertTrue(this.vpaFlowCriteria.flowExists("proj3", null));
    Assert.assertFalse(this.vpaFlowCriteria.flowExists("proj2", null));
    Assert.assertFalse(this.vpaFlowCriteria.flowExists("proj1", null));
  }

  @Test
  public void testFlowNameFilter() throws Exception {
    Assert.assertTrue(this.vpaFlowCriteria.flowExists("proj1", "flow1"));
    Assert.assertTrue(this.vpaFlowCriteria.flowExists("proj2", "flow2"));
    Assert.assertTrue(this.vpaFlowCriteria.flowExists("proj1", "flow3"));
    Assert.assertFalse(this.vpaFlowCriteria.flowExists("proj2", "flow3"));
    Assert.assertFalse(this.vpaFlowCriteria.flowExists("proj1", "flow2"));
  }
}
