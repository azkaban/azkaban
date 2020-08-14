package azkaban.execapp.event;

import static azkaban.Constants.JobCallbackProperties.JOBCALLBACK_CONNECTION_REQUEST_TIMEOUT;
import static azkaban.Constants.JobCallbackProperties.JOBCALLBACK_CONNECTION_TIMEOUT;
import static azkaban.Constants.JobCallbackProperties.JOBCALLBACK_RESPONSE_WAIT_TIMEOUT;
import static azkaban.Constants.JobCallbackProperties.JOBCALLBACK_SOCKET_TIMEOUT;
import static azkaban.Constants.JobCallbackProperties.JOBCALLBACK_THREAD_POOL_SIZE;

import azkaban.utils.Props;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.FutureRequestExecutionMetrics;
import org.apache.http.impl.client.FutureRequestExecutionService;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpRequestFutureTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for making the job callback HTTP requests.
 *
 * One of the requirements is to log out the request information and response using the given
 * logger, which should be the job logger.
 *
 * @author hluu
 */
public class JobCallbackRequestMaker {

  private static final Logger logger = LoggerFactory.getLogger(JobCallbackRequestMaker.class);

  private static final int DEFAULT_TIME_OUT_MS = 3000;
  private static final int DEFAULT_RESPONSE_WAIT_TIME_OUT_MS = 5000;
  private static final int MAX_RESPONSE_LINE_TO_PRINT = 50;

  private static final int DEFAULT_THREAD_POOL_SIZE = 10;

  private static JobCallbackRequestMaker instance;
  private static boolean isInitialized = false;

  private final FutureRequestExecutionService futureRequestExecutionService;
  private int responseWaitTimeoutMS = -1;

  private JobCallbackRequestMaker(final Props props) {

    final int connectionRequestTimeout =
        props.getInt(JOBCALLBACK_CONNECTION_REQUEST_TIMEOUT, DEFAULT_TIME_OUT_MS);

    final int connectionTimeout = props.getInt(JOBCALLBACK_CONNECTION_TIMEOUT, DEFAULT_TIME_OUT_MS);

    final int socketTimeout = props.getInt(JOBCALLBACK_SOCKET_TIMEOUT, DEFAULT_TIME_OUT_MS);

    this.responseWaitTimeoutMS =
        props.getInt(JOBCALLBACK_RESPONSE_WAIT_TIMEOUT, DEFAULT_RESPONSE_WAIT_TIME_OUT_MS);

    logger.info("responseWaitTimeoutMS: " + this.responseWaitTimeoutMS);

    final RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectionRequestTimeout(connectionRequestTimeout)
            .setConnectTimeout(connectionTimeout)
            .setSocketTimeout(socketTimeout).build();

    logger.info("Global request configuration " + requestConfig.toString());

    final HttpClient httpClient =
        HttpClientBuilder.create().setDefaultRequestConfig(requestConfig)
            .build();

    final int jobCallbackThreadPoolSize =
        props.getInt(JOBCALLBACK_THREAD_POOL_SIZE, DEFAULT_THREAD_POOL_SIZE);
    logger.info("Jobcall thread pool size: " + jobCallbackThreadPoolSize);

    final ExecutorService executorService =
        Executors.newFixedThreadPool(jobCallbackThreadPoolSize,
            new ThreadFactoryBuilder().setNameFormat("azk-callback-pool-%d").build());
    this.futureRequestExecutionService =
        new FutureRequestExecutionService(httpClient, executorService);
  }

  public static void initialize(final Props props) {
    if (props == null) {
      throw new NullPointerException("props argument can't be null");
    }

    if (isInitialized) {
      return;
    }

    instance = new JobCallbackRequestMaker(props);
    isInitialized = true;
    logger.info("Initialization for " + JobCallbackRequestMaker.class.getName()
        + " is completed");
  }

  public static boolean isInitialized() {
    return isInitialized;
  }

  public static JobCallbackRequestMaker getInstance() {
    if (!isInitialized) {
      throw new IllegalStateException(JobCallbackRequestMaker.class.getName()
          + " hasn't initialzied");
    }
    return instance;
  }

  public FutureRequestExecutionMetrics getJobcallbackMetrics() {
    return this.futureRequestExecutionService.metrics();
  }

  public void makeHttpRequest(final String jobId, final Logger logger,
      final List<HttpRequestBase> httpRequestList) {
    if (httpRequestList == null || httpRequestList.isEmpty()) {
      logger.info("No HTTP requests to make for: " + jobId);
      return;
    }

    for (final HttpRequestBase httpRequest : httpRequestList) {
      logger.debug("Job callback http request for " + jobId + ": " + httpRequest.toString());
      logger.debug("headers [");
      for (final Header header : httpRequest.getAllHeaders()) {
        logger.debug(String.format("  %s : %s", header.getName(),
            header.getValue()));
      }
      logger.debug("]");

      final HttpRequestFutureTask<Integer> task =
          this.futureRequestExecutionService.execute(httpRequest,
              HttpClientContext.create(), new LoggingResponseHandler(logger));

      try {
        // get with timeout
        final Integer statusCode =
            task.get(this.responseWaitTimeoutMS, TimeUnit.MILLISECONDS);

        logger.info("http callback status code for " + jobId + ": " + statusCode);
      } catch (final TimeoutException timeOutEx) {
        logger
            .warn("Job callback target took longer "
                    + (this.responseWaitTimeoutMS / 1000) + " seconds to respond for: " + jobId,
                timeOutEx);
      } catch (final ExecutionException ee) {
        if (ee.getCause() instanceof SocketTimeoutException) {
          logger.warn("Job callback target took longer "
              + (this.responseWaitTimeoutMS / 1000) + " seconds to respond", ee);
        } else {
          logger.warn(
              "Failed to execute job callback for: " + jobId,
              ee);
        }
      } catch (final Throwable e) {
        logger.warn(
            "Failed to execute job callback for: " + jobId,
            e);
      }
    }
  }

  /**
   * Response handler for logging job callback response using the given logger instance
   *
   * @author hluu
   */
  private static final class LoggingResponseHandler implements
      ResponseHandler<Integer> {

    private final Logger logger;

    public LoggingResponseHandler(final Logger logger) {
      if (logger == null) {
        throw new NullPointerException("Argument logger can't be null");
      }
      this.logger = logger;
    }

    @Override
    public Integer handleResponse(final HttpResponse response)
        throws ClientProtocolException, IOException {

      final int statusCode = response.getStatusLine().getStatusCode();
      BufferedReader bufferedReader = null;

      try {
        final HttpEntity responseEntity = response.getEntity();
        if (responseEntity != null) {
          bufferedReader =
              new BufferedReader(new InputStreamReader(
                  responseEntity.getContent(), StandardCharsets.UTF_8));

          String line = "";
          int lineCount = 0;
          this.logger.info("HTTP response [");
          while ((line = bufferedReader.readLine()) != null) {
            this.logger.info(line);
            lineCount++;
            if (lineCount > MAX_RESPONSE_LINE_TO_PRINT) {
              break;
            }
          }
          this.logger.info("]");
        } else {
          this.logger.info("No response");
        }

      } catch (final Throwable t) {
        this.logger.warn(
            "Encountered error while logging out job callback response", t);
      } finally {
        if (bufferedReader != null) {
          try {
            bufferedReader.close();
          } catch (final IOException ex) {
            // don't care
          }
        }
      }
      return statusCode;

    }
  }

}
