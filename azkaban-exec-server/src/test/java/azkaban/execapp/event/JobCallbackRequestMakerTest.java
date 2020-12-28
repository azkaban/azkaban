package azkaban.execapp.event;

import static azkaban.Constants.JobCallbackProperties.JOBCALLBACK_CONNECTION_REQUEST_TIMEOUT;
import static azkaban.Constants.JobCallbackProperties.JOBCALLBACK_CONNECTION_TIMEOUT;
import static azkaban.Constants.JobCallbackProperties.JOBCALLBACK_SOCKET_TIMEOUT;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_EXECUTION_ID_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_FLOW_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_STATUS_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_PROJECT_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_SERVER_TOKEN;

import azkaban.jobcallback.JobCallbackConstants;
import azkaban.jobcallback.JobCallbackStatusEnum;
import azkaban.utils.Props;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobCallbackRequestMakerTest {

  private static final Logger logger = LoggerFactory.getLogger(JobCallbackRequestMakerTest.class);

  private static final String SLEEP_DURATION_PARAM = "sleepDuration";
  private static final String STATUS_CODE_PARAM = "returnedStatusCode";

  private static final String SERVER_NAME = "localhost:9999";
  private static final String PROJECT_NANE = "PROJECTX";
  private static final String FLOW_NANE = "FLOWX";
  private static final String JOB_NANE = "JOBX";
  private static final String EXECUTION_ID = "1234";

  private static final int PORT_NUMBER = 8989;

  private static JobCallbackRequestMaker jobCBMaker;

  private static Map<String, String> contextInfo;

  private static Server embeddedJettyServer;

  @BeforeClass
  public static void setup() throws Exception {
    final Props props = new Props();
    final int timeout = 50;
    props.put(JOBCALLBACK_CONNECTION_REQUEST_TIMEOUT, timeout);
    props.put(JOBCALLBACK_CONNECTION_TIMEOUT, timeout);
    props.put(JOBCALLBACK_SOCKET_TIMEOUT, timeout);
    JobCallbackRequestMaker.initialize(props);
    jobCBMaker = JobCallbackRequestMaker.getInstance();

    contextInfo = new HashMap<>();
    contextInfo.put(CONTEXT_SERVER_TOKEN, SERVER_NAME);
    contextInfo.put(CONTEXT_PROJECT_TOKEN, PROJECT_NANE);
    contextInfo.put(CONTEXT_FLOW_TOKEN, FLOW_NANE);
    contextInfo.put(CONTEXT_EXECUTION_ID_TOKEN, EXECUTION_ID);
    contextInfo.put(CONTEXT_JOB_TOKEN, JOB_NANE);
    contextInfo.put(CONTEXT_JOB_STATUS_TOKEN, JobCallbackStatusEnum.STARTED.name());

    embeddedJettyServer = new Server(PORT_NUMBER);

    final ServletContextHandler context = new ServletContextHandler(embeddedJettyServer, "/",
        ServletContextHandler.SESSIONS);
    context.addServlet(new ServletHolder(new DelayServlet()), "/delay");

    System.out.println("Start server");
    embeddedJettyServer.start();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    System.out.println("Shutting down server");
    if (embeddedJettyServer != null) {
      embeddedJettyServer.stop();
      embeddedJettyServer.destroy();
    }
  }

  private String buildUrlForDelay(final int delay) {
    return "http://localhost:" + PORT_NUMBER + "/delay?" + SLEEP_DURATION_PARAM
        + "=" + delay;
  }

  private String buildUrlForStatusCode(final int sc) {
    return "http://localhost:" + PORT_NUMBER + "/delay?" + STATUS_CODE_PARAM
        + "=" + sc;
  }

  @Test(timeout = 4000)
  public void basicGetTest() {
    final Props props = new Props();
    final String url = buildUrlForDelay(1);

    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);

    final List<HttpRequestBase> httpRequestList =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    jobCBMaker.makeHttpRequest(JOB_NANE, logger, httpRequestList);
  }

  @Test(timeout = 4000)
  public void simulateNotOKStatusCodeTest() {
    final Props props = new Props();
    final String url = buildUrlForStatusCode(404);
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);

    final List<HttpRequestBase> httpRequestList =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    jobCBMaker.makeHttpRequest(JOB_NANE, logger, httpRequestList);
  }

  @Test(timeout = 4000)
  public void unResponsiveGetTest() {
    final Props props = new Props();
    final String url = buildUrlForDelay(10);
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);

    final List<HttpRequestBase> httpRequestList =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    jobCBMaker.makeHttpRequest(JOB_NANE, logger, httpRequestList);
  }

  @Test(timeout = 4000)
  public void basicPostTest() {
    final Props props = new Props();
    final String url = buildUrlForDelay(1);
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);
    props.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);
    props.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.body",
        "This is it");

    final List<HttpRequestBase> httpRequestList =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    jobCBMaker.makeHttpRequest(JOB_NANE, logger, httpRequestList);
  }

  private static class DelayServlet extends HttpServlet {

    @Override
    public void doGet(final HttpServletRequest req, final HttpServletResponse resp)
        throws ServletException, IOException {

      logger.info("Get get request: " + req.getRequestURI());
      logger.info("Get get request params: " + req.getParameterMap());

      final long start = System.currentTimeMillis();
      String responseMessage = handleDelay(req);
      logger
          .info("handleDelay elapse: " + (System.currentTimeMillis() - start));

      responseMessage = handleSimulatedStatusCode(req, resp, responseMessage);

      final Writer writer = resp.getWriter();
      writer.write(responseMessage);
      writer.close();
    }

    private String handleSimulatedStatusCode(final HttpServletRequest req,
        final HttpServletResponse resp, String responseMessge) {
      final String returnedStatusCodeStr = req.getParameter(STATUS_CODE_PARAM);
      if (returnedStatusCodeStr != null) {
        final int statusCode = Integer.parseInt(returnedStatusCodeStr);
        responseMessge = "Not good";
        resp.setStatus(statusCode);
      }
      return responseMessge;
    }

    private String handleDelay(final HttpServletRequest req) {
      final String sleepParamValue = req.getParameter(SLEEP_DURATION_PARAM);
      if (sleepParamValue != null) {
        final long howLongMS =
            TimeUnit.MILLISECONDS.convert(Integer.parseInt(sleepParamValue),
                TimeUnit.SECONDS);

        logger.info("Delay for: " + howLongMS);

        try {
          Thread.sleep(howLongMS);
          return "Voila!!";
        } catch (final InterruptedException e) {
          // don't care
          return e.getMessage();
        }
      }
      return "";
    }

    @Override
    public void doPost(final HttpServletRequest req, final HttpServletResponse resp)
        throws ServletException, IOException {
      logger.info("Get post request: " + req.getRequestURI());
      logger.info("Get post request params: " + req.getParameterMap());

      final BufferedReader reader = req.getReader();
      String line = null;
      while ((line = reader.readLine()) != null) {
        logger.info("post body: " + line);
      }
      reader.close();

      String responseMessage = handleDelay(req);
      responseMessage = handleSimulatedStatusCode(req, resp, responseMessage);

      final Writer writer = resp.getWriter();
      writer.write(responseMessage);
      writer.close();
    }
  }
}
