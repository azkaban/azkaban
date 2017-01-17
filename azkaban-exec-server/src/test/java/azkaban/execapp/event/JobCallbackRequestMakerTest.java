package azkaban.execapp.event;

import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_EXECUTION_ID_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_FLOW_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_STATUS_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_PROJECT_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_SERVER_TOKEN;

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
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import azkaban.jobcallback.JobCallbackConstants;
import azkaban.jobcallback.JobCallbackStatusEnum;
import azkaban.utils.Props;

public class JobCallbackRequestMakerTest {

  private static final Logger logger = Logger
      .getLogger(JobCallbackRequestMakerTest.class);

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
    try {
      JobCallbackRequestMaker.initialize(new Props());
      jobCBMaker = JobCallbackRequestMaker.getInstance();

      contextInfo = new HashMap<String, String>();
      contextInfo.put(CONTEXT_SERVER_TOKEN, SERVER_NAME);
      contextInfo.put(CONTEXT_PROJECT_TOKEN, PROJECT_NANE);
      contextInfo.put(CONTEXT_FLOW_TOKEN, FLOW_NANE);
      contextInfo.put(CONTEXT_EXECUTION_ID_TOKEN, EXECUTION_ID);
      contextInfo.put(CONTEXT_JOB_TOKEN, JOB_NANE);
      contextInfo.put(CONTEXT_JOB_STATUS_TOKEN, JobCallbackStatusEnum.STARTED.name());

      embeddedJettyServer = new Server(PORT_NUMBER);

      Context context = new Context(embeddedJettyServer, "/", Context.SESSIONS);
      context.addServlet(new ServletHolder(new DelayServlet()), "/delay");

      System.out.println("Start server");
      embeddedJettyServer.start();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    System.out.println("Shutting down server");
    if (embeddedJettyServer != null) {
      embeddedJettyServer.stop();
      embeddedJettyServer.destroy();
    }
  }

  private static class DelayServlet extends HttpServlet {

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      logger.info("Get get request: " + req.getRequestURI());
      logger.info("Get get request params: " + req.getParameterMap());

      long start = System.currentTimeMillis();
      String responseMessage = handleDelay(req);
      logger
          .info("handleDelay elapse: " + (System.currentTimeMillis() - start));

      responseMessage = handleSimulatedStatusCode(req, resp, responseMessage);

      Writer writer = resp.getWriter();
      writer.write(responseMessage);
      writer.close();
    }

    private String handleSimulatedStatusCode(HttpServletRequest req,
        HttpServletResponse resp, String responseMessge) {
      String returnedStatusCodeStr = req.getParameter(STATUS_CODE_PARAM);
      if (returnedStatusCodeStr != null) {
        int statusCode = Integer.parseInt(returnedStatusCodeStr);
        responseMessge = "Not good";
        resp.setStatus(statusCode);
      }
      return responseMessge;
    }

    private String handleDelay(HttpServletRequest req) {
      String sleepParamValue = req.getParameter(SLEEP_DURATION_PARAM);
      if (sleepParamValue != null) {
        long howLongMS =
            TimeUnit.MILLISECONDS.convert(Integer.parseInt(sleepParamValue),
                TimeUnit.SECONDS);

        logger.info("Delay for: " + howLongMS);

        try {
          Thread.sleep(howLongMS);
          return "Voila!!";
        } catch (InterruptedException e) {
          // don't care
          return e.getMessage();
        }
      }
      return "";
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      logger.info("Get post request: " + req.getRequestURI());
      logger.info("Get post request params: " + req.getParameterMap());

      BufferedReader reader = req.getReader();
      String line = null;
      while ((line = reader.readLine()) != null) {
        logger.info("post body: " + line);
      }
      reader.close();

      String responseMessage = handleDelay(req);
      responseMessage = handleSimulatedStatusCode(req, resp, responseMessage);

      Writer writer = resp.getWriter();
      writer.write(responseMessage);
      writer.close();
    }
  }

  private String buildUrlForDelay(int delay) {
    return "http://localhost:" + PORT_NUMBER + "/delay?" + SLEEP_DURATION_PARAM
        + "=" + delay;
  }

  private String buildUrlForStatusCode(int sc) {
    return "http://localhost:" + PORT_NUMBER + "/delay?" + STATUS_CODE_PARAM
        + "=" + sc;
  }

  @Test(timeout = 4000)
  public void basicGetTest() {
    Props props = new Props();
    String url = buildUrlForDelay(1);

    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);

    List<HttpRequestBase> httpRequestList =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    jobCBMaker.makeHttpRequest(JOB_NANE, logger, httpRequestList);
  }

  @Test(timeout = 4000)
  public void simulateNotOKStatusCodeTest() {
    Props props = new Props();
    String url = buildUrlForStatusCode(404);
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);

    List<HttpRequestBase> httpRequestList =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    jobCBMaker.makeHttpRequest(JOB_NANE, logger, httpRequestList);
  }

  @Test(timeout = 4000)
  public void unResponsiveGetTest() {
    Props props = new Props();
    String url = buildUrlForDelay(10);
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);

    List<HttpRequestBase> httpRequestList =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    jobCBMaker.makeHttpRequest(JOB_NANE, logger, httpRequestList);
  }

  @Test(timeout = 4000)
  public void basicPostTest() {
    Props props = new Props();
    String url = buildUrlForDelay(1);
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.method",
        JobCallbackConstants.HTTP_POST);
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.body",
        "This is it");

    List<HttpRequestBase> httpRequestList =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    jobCBMaker.makeHttpRequest(JOB_NANE, logger, httpRequestList);
  }
}
