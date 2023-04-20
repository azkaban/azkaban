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

package azkaban.alert;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.project.Project;
import azkaban.sla.SlaOption;
import azkaban.utils.Emailer;
import azkaban.utils.HTMLFormElement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface Alerter {

  void alertOnSuccess(ExecutableFlow exflow) throws Exception;

  void alertOnError(ExecutableFlow exflow, String... extraReasons) throws Exception;

  void alertOnFirstError(ExecutableFlow exflow) throws Exception;

  @Deprecated
  default void alertOnSla(SlaOption slaOption, String slaMessage) throws Exception { }

  default void alertOnSla(ExecutableFlow exflow, SlaOption slaOption) throws Exception {
    final String slaMessage = Emailer.createSlaMessage(exflow, slaOption, getAzkabanURL());
    alertOnSla(slaOption, slaMessage);
  }

  void alertOnFailedUpdate(Executor executor, List<ExecutableFlow> executions,
      ExecutorManagerException e);

  void alertOnFailedExecutorHealthCheck(Executor executor,
      List<ExecutableFlow> executions,
      ExecutorManagerException e, List<String> alertEmails);

  void alertOnJobPropertyOverridden(Project project, Flow flow, Map<String, Object> metaData);

  @Deprecated
  default String getAzkabanURL() {
    return "";
  }

  /**
   * Parameters users should set to enable alerts on SLA misses via Web UI. Currently used to
   * render the SLA definition page.
   */
  default List<HTMLFormElement> getViewParameters() {
    return Collections.emptyList();
  }
}
