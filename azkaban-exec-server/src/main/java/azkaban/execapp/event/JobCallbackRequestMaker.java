package azkaban.execapp.event;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
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
import org.apache.log4j.Logger;

import azkaban.utils.Props;

/**
 * Responsible for making the job callback HTTP requests.
 * 
 * One of the requirements is to log out the request information and response
 * using the given logger, which should be the job logger.
 * 
 * @author hluu
 *
 */
public class JobCallbackRequestMaker {

  private static final Logger logger = Logger
      .getLogger(JobCallbackRequestMaker.class);

  private static final int DEFAULT_TIME_OUT_MS = 3000;
  private static final int DEFAULT_RESPONSE_WAIT_TIME_OUT_MS = 5000;
  private static final int MAX_RESPONSE_LINE_TO_PRINT = 50;

  private static final int DEFAULT_THREAD_POOL_SIZE = 10;

  private static JobCallbackRequestMaker instance;
  private static boolean isInitialized = false;

  private FutureRequestExecutionService futureRequestExecutionService;
  private int responseWaitTimeoutMS = -1;

  public static void initialize(Props props) {
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

  private JobCallbackRequestMaker(Props props) {

    int connectionRequestTimeout =
        props.getInt("jobcallback.connection.request.timeout",
            DEFAULT_TIME_OUT_MS);

    int connectionTimeout =
        props.getInt("jobcallback.connection.timeout", DEFAULT_TIME_OUT_MS);

    int socketTimeout =
        props.getInt("jobcallback.socket.timeout", DEFAULT_TIME_OUT_MS);

    responseWaitTimeoutMS =
        props.getInt("jobcallback.response.wait.timeout",
            DEFAULT_RESPONSE_WAIT_TIME_OUT_MS);

    logger.info("responseWaitTimeoutMS: " + responseWaitTimeoutMS);

    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectionRequestTimeout(connectionRequestTimeout)
            .setConnectTimeout(connectionTimeout)
            .setSocketTimeout(socketTimeout).build();

    logger.info("Global request configuration " + requestConfig.toString());

    HttpClient httpClient =
        HttpClientBuilder.create().setDefaultRequestConfig(requestConfig)
            .build();

    int jobCallbackThreadPoolSize =
        props.getInt("jobcallback.thread.pool.size", DEFAULT_THREAD_POOL_SIZE);
    logger.info("Jobcall thread pool size: " + jobCallbackThreadPoolSize);

    ExecutorService executorService =
        Executors.newFixedThreadPool(jobCallbackThreadPoolSize);
    futureRequestExecutionService =
        new FutureRequestExecutionService(httpClient, executorService);
  }

  public FutureRequestExecutionMetrics getJobcallbackMetrics() {
    return futureRequestExecutionService.metrics();
  }

  public void makeHttpRequest(String jobId, Logger logger,
      List<HttpRequestBase> httpRequestList) {

    if (httpRequestList == null || httpRequestList.isEmpty()) {
      logger.info("No HTTP requests to make");
      return;
    }

    for (HttpRequestBase httpRequest : httpRequestList) {

      logger.info("Job callback http request: " + httpRequest.toString());
      logger.info("headers [");
      for (Header header : httpRequest.getAllHeaders()) {
        logger.info(String.format("  %s : %s", header.getName(),
            header.getValue()));
      }
      logger.info("]");

      HttpRequestFutureTask<Integer> task =
          futureRequestExecutionService.execute(httpRequest,
              HttpClientContext.create(), new LoggingResponseHandler(logger));

      try {
        // get with timeout
        Integer statusCode =
            task.get(responseWaitTimeoutMS, TimeUnit.MILLISECONDS);

        logger.info("http callback status code: " + statusCode);
      } catch (TimeoutException timeOutEx) {
        logger
            .warn("Job callback target took longer "
                + (responseWaitTimeoutMS / 1000) + " seconds to respond",
                timeOutEx);
      } catch (ExecutionException ee) {
        if (ee.getCause() instanceof SocketTimeoutException) {
          logger.warn("Job callback target took longer "
              + (responseWaitTimeoutMS / 1000) + " seconds to respond", ee);
        } else {
          logger.warn(
              "Encountered error while waiting for job callback to complete",
              ee);
        }
      } catch (Throwable e) {
        logger.warn(
            "Encountered error while waiting for job callback to complete", e);
      }
    }
  }

  /**
   * Response handler for logging job callback response using the given logger
   * instance
   * 
   * @author hluu
   *
   */
  private final class LoggingResponseHandler implements
      ResponseHandler<Integer> {

    private Logger logger;

    public LoggingResponseHandler(Logger logger) {
      if (logger == null) {
        throw new NullPointerException("Argument logger can't be null");
      }
      this.logger = logger;
    }

    public Integer handleResponse(final HttpResponse response)
        throws ClientProtocolException, IOException {

      int statusCode = response.getStatusLine().getStatusCode();
      BufferedReader bufferedReader = null;

      try {
        HttpEntity responseEntity = response.getEntity();
        if (responseEntity != null) {
          bufferedReader =
              new BufferedReader(new InputStreamReader(
                  responseEntity.getContent()));

          String line = "";
          int lineCount = 0;
          logger.info("HTTP response [");
          while ((line = bufferedReader.readLine()) != null) {
            logger.info(line);
            lineCount++;
            if (lineCount > MAX_RESPONSE_LINE_TO_PRINT) {
              break;
            }
          }
          logger.info("]");
        } else {
          logger.info("No response");
        }

      } catch (Throwable t) {
        logger.warn(
            "Encountered error while logging out job callback response", t);
      } finally {
        if (bufferedReader != null) {
          try {
            bufferedReader.close();
          } catch (IOException ex) {
            // don't care
          }
        }
      }
      return statusCode;

    }
  }

}
