/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.jobtype.hiveutils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Guice-like module for creating a Hive instance. Easily turned back into a
 * full Guice module when we have need of it.
 */
class HiveModule {
  /**
   * Return a Driver that's connected to the real, honest-to-goodness Hive
   *
   * @TODO: Better error checking
   * @return Driver that's connected to Hive
   */
  Driver provideHiveDriver() {
    HiveConf hiveConf = provideHiveConf();
    SessionState.start(hiveConf);

    return new Driver(hiveConf);
  }

  HiveConf provideHiveConf() {
    return new HiveConf(SessionState.class);
  }

  protected void configure() { /* Nothing to do */
  }
}
