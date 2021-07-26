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
package azkaban.executor.container.watch;

import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains methods for logging of Kubernetes watch events.
 *
 * This currently utilizes the {@code toString()} method of {@link V1Pod} which generates
 * a human-friendly string representation of the event.
 * However, there is value in logging the raw json representation of the events received
 * from the Kubernetes APIServer as they will be more useful for scripted analyses, something we
 * can consider in future.
 *
 * Also note that having a dedicated class for logging of watch events makes it easier to define
 * the logging properties which can redirect these events to a dedicated log file.
 */
public class WatchEventLogger {
  private static final Logger logger = LoggerFactory.getLogger(WatchEventLogger.class);

  public static void logWatchEvent(AzPodStatusMetadata event, String message) {
    try {
      logger.info(new StringBuffer(message)
          .append(System.lineSeparator())
          .append(event.getPodWatchEvent().object.toString())
          .toString());
    } catch (Exception e) {
      logger.error("Unexpected exception while logging watch event for pod " + event.getPodName()
          , e);
    }
  }
}
