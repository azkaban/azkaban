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

package azkaban.trigger;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import azkaban.executor.ExecutionOptions;
import azkaban.trigger.builtin.ExecuteFlowAction;
import azkaban.utils.Props;

public class ExecuteFlowActionTest {

  @Ignore @Test
  public void jsonConversionTest() throws Exception {
    ActionTypeLoader loader = new ActionTypeLoader();
    loader.init(new Props());

    ExecutionOptions options = new ExecutionOptions();
    List<Object> disabledJobs = new ArrayList<Object>();
    options.setDisabledJobs(disabledJobs);

    ExecuteFlowAction executeFlowAction =
        new ExecuteFlowAction("ExecuteFlowAction", 1, "testproject",
            "testflow", "azkaban", options, null);

    Object obj = executeFlowAction.toJson();

    ExecuteFlowAction action =
        (ExecuteFlowAction) loader.createActionFromJson(ExecuteFlowAction.type,
            obj);
    assertTrue(executeFlowAction.getProjectId() == action.getProjectId());
    assertTrue(executeFlowAction.getFlowName().equals(action.getFlowName()));
    assertTrue(executeFlowAction.getSubmitUser().equals(action.getSubmitUser()));
  }

}
