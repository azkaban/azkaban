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
package azkaban.flowtrigger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import azkaban.flowtrigger.database.FlowTriggerInstanceLoader;
import org.junit.BeforeClass;
import org.junit.Test;


public class DependencyInstanceProcessorTest {

  private static FlowTriggerInstanceLoader triggerInstLoader;
  private static DependencyInstanceProcessor processor;


  @BeforeClass
  public static void setup() throws Exception {
    triggerInstLoader = mock(FlowTriggerInstanceLoader.class);
    doNothing().when(triggerInstLoader).updateDependencyExecutionStatus(any());
    processor = new DependencyInstanceProcessor(triggerInstLoader);
  }


  @Test
  public void testStatusUpdate() {
    final DependencyInstance depInst = new DependencyInstance("dep1", System.currentTimeMillis()
        , 0, null, Status.RUNNING, CancellationCause.NONE);
    processor.processStatusUpdate(depInst);
    verify(triggerInstLoader).updateDependencyExecutionStatus(depInst);
  }
}
