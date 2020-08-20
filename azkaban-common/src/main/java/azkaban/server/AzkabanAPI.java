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

package azkaban.server;

import azkaban.metrics.AzkabanAPIMetrics;

/**
 * Represents an operation (for example: ajax=executeFlow) that can be performed programmatically
 * (using curl command for example) and doesn't return HTML, usually just data.
 */
public class AzkabanAPI {

  private final String requestParameter;
  private final String parameterValue;
  private AzkabanAPIMetrics metrics;

  public AzkabanAPI(final String requestParameter, final String parameterValue) {
    this.requestParameter = requestParameter;
    this.parameterValue = parameterValue;
    this.metrics = null;
  }

  public String getRequestParameter() {
    return this.requestParameter;
  }

  public String getParameterValue() {
    return this.parameterValue;
  }


  public AzkabanAPIMetrics getMetrics() {
    return this.metrics;
  }

  public void setMetrics(final AzkabanAPIMetrics metrics) {
    this.metrics = metrics;
  }
}
