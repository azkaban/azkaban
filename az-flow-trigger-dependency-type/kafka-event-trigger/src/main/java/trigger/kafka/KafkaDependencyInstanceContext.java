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
import azkaban.flowtrigger.DependencyInstanceConfig;
import azkaban.flowtrigger.DependencyInstanceContext;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import trigger.kafka.Constants.DependencyInstanceConfigKey;

/**
 * KafkaDependencyInstanceContext maintains attributes of a running instance of kafka dependency.
 */
public class KafkaDependencyInstanceContext implements DependencyInstanceContext {
  private final static Logger log = LoggerFactory.getLogger(KafkaDependencyInstanceContext.class);
  private final KafkaDependencyCheck depCheck;
  private final DependencyInstanceCallback callback;
  private final String topicName;
  private final String regexMatch;
  private final String depName;

  public KafkaDependencyInstanceContext(final DependencyInstanceConfig config,
      final KafkaDependencyCheck dependencyCheck, final DependencyInstanceCallback callback) {
    this.topicName = config.get(DependencyInstanceConfigKey.TOPIC);
    this.callback = callback;
    this.depCheck = dependencyCheck;
    this.regexMatch = config.get(DependencyInstanceConfigKey.MATCH);
    this.depName = config.get(DependencyInstanceConfigKey.NAME);
  }

  @Override
  public void cancel() {
    log.info(String.format("Canceling dependency %s", this));
    this.depCheck.remove(this);
    this.callback.onCancel(this);
  }

  public String getRegexMatch() {

    return this.regexMatch;
  }

  public String getTopicName() {

    return this.topicName;
  }

  public DependencyInstanceCallback getCallback() {

    return this.callback;
  }
}
