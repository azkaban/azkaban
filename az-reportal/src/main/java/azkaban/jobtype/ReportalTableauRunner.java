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

package azkaban.jobtype;

import azkaban.reportal.util.tableau.Countdown;
import azkaban.reportal.util.tableau.Result;
import azkaban.reportal.util.tableau.URLResponse;
import java.time.Duration;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ReportalTableauRunner extends ReportalAbstractRunner {

  private static final String WORKBOOK = "workbook.name";
  private static final String TIMEOUT = "max.running.mins";
  private static final String TABLEAU_URL = "tableau.url";
  private static final Logger logger = Logger.getLogger(ReportalTableauRunner.class);

  public ReportalTableauRunner(final String jobName, final Properties props) {
    super(props);
  }

  private void refreshExtract(final String tableauUrl, final String workbook) throws Exception {
    final URLResponse urlResponse = new URLResponse(tableauUrl, URLResponse.Path.REFRESH_EXTRACT,
        workbook);
    logger.info(urlResponse.getContents());
  }

  private Result getLastExtractStatus(final String tableauUrl, final String workbook, final Duration
      maxRunningDuration)
      throws Exception {
    final URLResponse urlResponse = new URLResponse(tableauUrl, URLResponse.Path
        .LAST_EXTRACT_STATUS,
        workbook);
    final Countdown countdown = new Countdown(maxRunningDuration.toMinutes());

    while (countdown.moreTimeRemaining()) {
      urlResponse.refreshContents();
      if (urlResponse.indicatesSuccess()) {
        logger.info(urlResponse.getContents());
        return (Result.SUCCESS);
      } else if (urlResponse.indicatesError()) {
        logger.error(urlResponse.getContents());
        return (Result.FAIL);
      }
      countdown.waitForOneMinute();
      logger.info("Re-attempting connection with workbook " + workbook + ".");
    }
    return Result.TIMEOUT;
  }


  private void handleRefreshFailure(final Result result, final String workbook) throws Exception {
    assert result == Result.FAIL || result == Result.TIMEOUT;
    final String errorMsg = result == Result.FAIL ? "failed to extract status from workbook " +
        workbook : "extract status from workbook " + workbook + " times out";
    throw new Exception(errorMsg);
  }

  @Override
  protected void runReportal() throws Exception {
    final String workbook = this.props.get(WORKBOOK);
    final int timeout = this.props.getInt(TIMEOUT);
    final String tableauUrl = this.props.get(TABLEAU_URL);
    /**
     * First refresh the extract
     * once the status is found, log the results and cancel the job if
     * the status was an error or a timeout
     */
    logger.info("Refreshing extract to workbook " + workbook);
    logger.info("Refreshing extract to tableauUrl " + tableauUrl);
    logger.info("Refreshing extract with timeout " + timeout);
    refreshExtract(tableauUrl, workbook);
    logger.info("Getting last extract status from workbook " + workbook + "\n"
        + "Will wait for Tableau to refresh for up to " + timeout + " mins");

    final Result result = getLastExtractStatus(tableauUrl, workbook, Duration.ofMinutes(timeout));

    logger.info("result:" + result.getMessage());
    if (result == Result.FAIL || result == Result.TIMEOUT) {
      handleRefreshFailure(result, workbook);
    }
  }

}
