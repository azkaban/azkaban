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


package trigger.kafka;

import azkaban.flowtrigger.DependencyInstanceCallback;
import azkaban.flowtrigger.DependencyInstanceContext;

/**
 * Implement callback function for testing purpose
 */
public class DependencyInstanceCallbackI implements DependencyInstanceCallback {


  public DependencyInstanceCallbackI() {}

  @Override
  public void onSuccess(DependencyInstanceContext dependencyInstanceContext) {
    System.out.println("SUCCESS");
  }

  @Override
  public void onCancel(DependencyInstanceContext dependencyInstanceContext) {
    System.out.println("CANCEL");
  }
}
