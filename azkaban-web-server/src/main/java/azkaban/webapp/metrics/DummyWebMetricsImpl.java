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

package azkaban.webapp.metrics;

import azkaban.utils.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No-op implementation of {@link WebMetrics} used when metrics are disabled or for unit tests.
 */
public class DummyWebMetricsImpl implements WebMetrics {

  private static final Logger logger = LoggerFactory.getLogger(WebMetricsImpl.class);

  @Override
  public void setUp(final DataProvider dataProvider) {
    logger.info("No metrics set up for Web server.");
  }

  @Override
  public void startReporting(final Props props) {
  }

  @Override
  public void markWebGetCall() {
  }

  @Override
  public void markWebPostCall() {
  }

  @Override
  public void setFetchLogLatency(final long milliseconds) {
  }
}
