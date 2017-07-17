/*
 * Copyright 2017 LinkedIn Corp.
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

package azkaban.metrics;

import azkaban.AzkabanCommonModuleConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class HdfsMetrics {

  private static final Logger log = LoggerFactory.getLogger(HdfsMetrics.class);
  private static final String PREFIX = "HDFS-quota-";

  private final MetricsManager metricsManager;
  private final FileSystem hdfs;
  private final String path;
  private final Duration interval;
  private final ContentSummaryWrapper metricsData = new ContentSummaryWrapper();

  @Inject
  public HdfsMetrics(final MetricsManager metricsManager, final FileSystem hdfs,
      final AzkabanCommonModuleConfig config) {
    this.metricsManager = metricsManager;
    this.hdfs = hdfs;
    this.path = config.getHdfsMetricsRootUri().getPath();
    this.interval = config.getHdfsMetricsPollInterval();
  }

  public void registerMetrics() {
    startFetcherThread();

    this.metricsManager.addGauge(PREFIX + "length", this.metricsData.length::get);
    this.metricsManager.addGauge(PREFIX + "fileCount", this.metricsData.fileCount::get);
    this.metricsManager.addGauge(PREFIX + "directoryCount", this.metricsData.directoryCount::get);
    this.metricsManager.addGauge(PREFIX + "quota", this.metricsData.quota::get);
    this.metricsManager.addGauge(PREFIX + "spaceConsumed", this.metricsData.spaceConsumed::get);
    this.metricsManager.addGauge(PREFIX + "spaceQuota", this.metricsData.spaceQuota::get);
  }

  /**
   * The fetcher thread is a daemon thread. It thread calls HDFS APIs on every interval.
   */
  private void startFetcherThread() {
    final Thread thread = new Thread(() -> {
      try {
        fetchHdfsMetrics();
        Thread.sleep(this.interval.toMillis());
      } catch (final Exception e) {
        log.error("HDFS Fetcher Thread interrupted. Continuing", e);
      }
    });
    thread.setDaemon(true);
    thread.start();
  }

  private void fetchHdfsMetrics() {
    try {
      this.metricsData.setValues(this.hdfs.getContentSummary(new Path(this.path)));
    } catch (final IOException e) {
      log.error("Error: Unable to fetch ContentSummary from HDFS instance", e);
    }
  }

  public static class ContentSummaryWrapper {

    private final AtomicLong length = new AtomicLong();
    private final AtomicLong fileCount = new AtomicLong();
    private final AtomicLong directoryCount = new AtomicLong();
    private final AtomicLong quota = new AtomicLong();
    private final AtomicLong spaceConsumed = new AtomicLong();
    private final AtomicLong spaceQuota = new AtomicLong();

    public void setValues(final ContentSummary contentSummary) {
      this.length.set(contentSummary.getLength());
      this.fileCount.set(contentSummary.getFileCount());
      this.directoryCount.set(contentSummary.getDirectoryCount());
      this.quota.set(contentSummary.getQuota());
      this.spaceConsumed.set(contentSummary.getSpaceConsumed());
      this.spaceQuota.set(contentSummary.getSpaceConsumed());
    }
  }
}
