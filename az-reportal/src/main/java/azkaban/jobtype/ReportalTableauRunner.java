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

  public static final String TIMEOUT = "tableau.timeout.minutes";
  public static final String TABLEAU_URL = "tableau.url";
  private static final Logger logger = Logger.getLogger(ReportalTableauRunner.class);
  private final int timeout;
  private final String tableauUrl;

  public ReportalTableauRunner(final String jobName, final Properties props) {
    super(props);
    this.timeout = this.props.getInt(TIMEOUT);
    this.tableauUrl = this.props.getString(TABLEAU_URL);
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
    final Countdown countdown = new Countdown(maxRunningDuration);

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
    final String workbook = this.jobQuery;
    /**
     * First refresh the extract
     * once the status is found, log the results and cancel the job if
     * the status was an error or a timeout
     */
    logger.info("Refreshing extract to workbook " + workbook);
    refreshExtract(this.tableauUrl, workbook);
    logger.info("Getting last extract status from workbook " + workbook + "\n"
        + "Will wait for Tableau to refresh for up to " + this.timeout + " mins");

    final Result result = getLastExtractStatus(this.tableauUrl, workbook, Duration.ofMinutes(
        this.timeout));

    logger.info("result:" + result.getMessage());
    if (result == Result.FAIL || result == Result.TIMEOUT) {
      handleRefreshFailure(result, workbook);
    }
  }

}
