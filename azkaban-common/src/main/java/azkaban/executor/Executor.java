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

package azkaban.executor;

import azkaban.utils.Utils;

import java.util.Date;
import java.util.Objects;

/**
 * Class to represent an AzkabanExecutorServer details for ExecutorManager
 *
 * @author gaggarwa
 */
public class Executor implements Comparable<Executor> {
  private final int id;
  private final String host;
  private final int port;
  private boolean isActive;
  // cached copy of the latest statistics from  the executor.
  private ExecutorInfo cachedExecutorStats;
  private Date lastStatsUpdatedTime;

  /**
   * <pre>
   * Construct an Executor Object
   * Note: port should be a within unsigned 2 byte
   * integer range
   * </pre>
   */
  public Executor(final int id, final String host, final int port, final boolean isActive) {
    if (!Utils.isValidPort(port)) {
      throw new IllegalArgumentException(String.format(
        "Invalid port number %d for host %s, executor_id %d", port, host, id));
    }

    this.id = id;
    this.host = host;
    this.port = port;
    this.isActive = isActive;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Executor)) {
      return false;
    }
    final Executor executor = (Executor) o;
    return this.id == executor.id &&
      this.port == executor.port &&
      Objects.equals(this.host, executor.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id, this.host, this.port);
  }

  @Override
  public String toString() {
    return String.format("%s:%s (id: %s), active=%s",
      null == this.host || this.host.length() == 0 ? "(empty)" : this.host,
      this.port, this.id, this.isActive);
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  public boolean isActive() {
    return this.isActive;
  }

  public void setActive(final boolean isActive) {
    this.isActive = isActive;
  }

  public int getId() {
    return this.id;
  }

  public ExecutorInfo getExecutorInfo() {
    return this.cachedExecutorStats;
  }

  public void setExecutorInfo(final ExecutorInfo info) {
    this.cachedExecutorStats = info;
    this.lastStatsUpdatedTime = new Date();
  }

  /**
   * Gets the timestamp when the executor info is last updated.
   *
   * @return date object represents the timestamp, null if the executor info of this specific
   * executor is never refreshed.
   */
  public Date getLastStatsUpdatedTime() {
    return this.lastStatsUpdatedTime;
  }

  @Override
  public int compareTo(final Executor o) {
    return null == o ? 1 : this.hashCode() - o.hashCode();
  }
}
