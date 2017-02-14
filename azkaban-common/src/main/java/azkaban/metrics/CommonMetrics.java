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
 */

package azkaban.metrics;

import azkaban.event.Event;
import azkaban.event.EventListener;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.Logger;


/**
 * This singleton class CommonMetrics is in charge of collecting varieties of metrics
 * from azkaban-common modules.
 */
public enum CommonMetrics implements EventListener {
  INSTANCE;

  private Meter uploadDBMeter;
  private Meter userDownloadMeter;
  private Meter azDownloadMeter;
  private Meter dbConnectionMeter;

  private static final Logger logger = Logger.getLogger(CommonMetrics.class);

  private CommonMetrics() {
  }

  public void addExecDBStateMetrics(MetricRegistry metrics) throws Exception {
    azDownloadMeter = metrics.meter("AZ-Download-meter");

    metrics.register("DB-AZ-Download-meter", new Gauge<Double>() {
      @Override
      public Double getValue() {
        return azDownloadMeter.getOneMinuteRate();
      }
    });
  }

  public void addWebDBStateMetrics(MetricRegistry metrics) throws Exception {
    uploadDBMeter = metrics.meter("upload-DB-Meter");
    userDownloadMeter = metrics.meter("user-Download-meter");

    metrics.register("DB-Uploading-Chunks-Meter", new Gauge<Double>() {
      @Override
      public Double getValue() {
        return uploadDBMeter.getOneMinuteRate();
      }
    });

    metrics.register("DB-user-Download-meter", new Gauge<Double>() {
      @Override
      public Double getValue() {
        return userDownloadMeter.getOneMinuteRate();
      }
    });

  }

  /*
   * This method must be synchronized since web server and executor call
   * this method simultaneously.
   */
  public synchronized void registerCommonMetrics(MetricRegistry metrics) {
    if(dbConnectionMeter == null) {
      dbConnectionMeter = metrics.meter("DB-Connection-meter");

      metrics.register("DB-Connection-Num-meter", new Gauge<Double>() {
        @Override
        public Double getValue() {
          return dbConnectionMeter.getOneMinuteRate();
        }
      });
    }
  }


  @Override
  public synchronized void handleEvent(Event event) {

    /**
     * TODO: Use switch to select event type.
     *
     */
    if (event.getType() == Event.Type.UPLOAD_FILE_CHUNK && uploadDBMeter != null) {
      uploadDBMeter.mark();
    } else if (event.getType() == Event.Type.USER_DOWNLOAD_FILE && userDownloadMeter != null) {
      userDownloadMeter.mark();
    } else if (event.getType() == Event.Type.AZ_DOWNLOAD_FILE && azDownloadMeter != null ) {
      azDownloadMeter.mark();
    } else if (event.getType() == Event.Type.DB_CONNECTION && dbConnectionMeter != null) {
      dbConnectionMeter.mark();
    }
  }

}
