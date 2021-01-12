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
 *
 */
package azkaban.storage;

import azkaban.utils.Props;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class is used as an abstract class for implementation of authentication mechanism for HDFS.
 */
public abstract class AbstractHdfsAuth {
  protected final boolean isSecurityEnabled;

  protected final Props props;

  protected UserGroupInformation loggedInUser = null;

  public AbstractHdfsAuth(final Props props, final Configuration conf) {
    this.props = props;
    UserGroupInformation.setConfiguration(conf);
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
  }

  /**
   * This method is used to authorize with HDFS.
   */
  public abstract void authorize();
}
