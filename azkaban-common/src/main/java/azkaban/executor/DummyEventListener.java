/*
 * Copyright 2021 LinkedIn Corp.
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
package azkaban.executor;

import azkaban.event.EventListener;
import org.apache.log4j.Logger;

/**
 * This class is a no-op implementation of {@link FlowStatusChangeEventListener}
 */
public class DummyEventListener implements EventListener {
  private static final Logger logger = Logger.getLogger(EventListener.class);

  @Override
  public void handleEvent(Object event) {
    logger.debug("Event listener is not been implemented");
  }
}