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

public class Constants {
  /**
   *  Define where the Kafka brocker is located.
   */
  public static class DependencyPluginConfigKey {
    public static final String KAKFA_BROKER_URL = "kafka.broker.url";
  }
  /**
   *  Required properties for dependencies
   */
  public static class DependencyInstanceConfigKey {
    public static final String NAME = "name";
    public static final String TOPIC = "topic";
    public static final String MATCH = "match";
  }

}
