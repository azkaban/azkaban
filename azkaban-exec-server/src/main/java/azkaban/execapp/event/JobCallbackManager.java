package azkaban.execapp.event;

import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_TOKEN;
import static azkaban.jobcallback.JobCallbackStatusEnum.COMPLETED;
import static azkaban.jobcallback.JobCallbackStatusEnum.FAILURE;
import static azkaban.jobcallback.JobCallbackStatusEnum.STARTED;
import static azkaban.jobcallback.JobCallbackStatusEnum.SUCCESS;

import java.net.InetAddress;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicHeader;
import org.apache.log4j.Logger;

import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.event.EventListener;
import azkaban.execapp.JobRunner;
import azkaban.execapp.jmx.JmxJobCallback;
import azkaban.execapp.jmx.JmxJobCallbackMBean;
import azkaban.executor.Status;
import azkaban.jobcallback.JobCallbackStatusEnum;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;

/**
 * Responsible processing job callback properties on job status change events.
 * 
 * When job callback properties are specified, they will be converted to HTTP
 * calls to execute. The HTTP requests will be made in asynchronous mode so the
 * caller to the handleEvent method will not be block. In addition, the HTTP
 * calls will be configured to time appropriately for connection request,
 * creating connection, and socket timeout.
 * 
 * The HTTP request and response will be logged out the job's log for debugging
 * and traceability purpose.
 * 
 * @author hluu
 *
 */
public class JobCallbackManager implements EventListener {

  private static final Logger logger = Logger
      .getLogger(JobCallbackManager.class);

  private static boolean isInitialized = false;
  private static JobCallbackManager instance;

  private static int maxNumCallBack = 3;

  private JmxJobCallbackMBean callbackMbean;
  private String azkabanHostName;
  private SimpleDateFormat gmtDateFormatter;

  private static final JobCallbackStatusEnum[] ON_COMPLETION_JOB_CALLBACK_STATUS =
      { SUCCESS, FAILURE, COMPLETED };

  public static void initialize(Props props) {
    if (isInitialized) {
      logger.info("Already initialized");
      return;
    }

    logger.info("Initializing");
    instance = new JobCallbackManager(props);

    isInitialized = true;
  }

  public static boolean isInitialized() {
    return isInitialized;
  }

  public static JobCallbackManager getInstance() {
    if (!isInitialized) {
      throw new IllegalStateException(JobCallbackManager.class.getName()
          + " has not been initialized");
    }
    return instance;
  }

  private JobCallbackManager(Props props) {
    maxNumCallBack = props.getInt("jobcallback.max_count", maxNumCallBack);

    // initialize the request maker
    JobCallbackRequestMaker.initialize(props);

    callbackMbean =
        new JmxJobCallback(JobCallbackRequestMaker.getInstance()
            .getJobcallbackMetrics());

    azkabanHostName = getAzkabanHostName(props);

    gmtDateFormatter = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
    gmtDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));

    logger.info("Initialization completed " + getClass().getName());
    logger.info("azkabanHostName " + azkabanHostName);
  }

  public JmxJobCallbackMBean getJmxJobCallbackMBean() {
    return callbackMbean;
  }

  @Override
  public void handleEvent(Event event) {
    if (!isInitialized) {
      return;
    }

    if (event.getRunner() instanceof JobRunner) {
      try {
        if (event.getType() == Event.Type.JOB_STARTED) {
          processJobCallOnStart(event);
        } else if (event.getType() == Event.Type.JOB_FINISHED) {
          processJobCallOnFinish(event);
        }
      } catch (Throwable e) {
        // Use job runner logger so user can see the issue in their job log
        JobRunner jobRunner = (JobRunner) event.getRunner();
        jobRunner.getLogger().error(
            "Encountered error while hanlding job callback event", e);
      }
    } else {
      logger.warn("((( Got an unsupported runner: "
          + event.getRunner().getClass().getName() + " )))");
    }

  }

  private void processJobCallOnFinish(Event event) {
    JobRunner jobRunner = (JobRunner) event.getRunner();
    EventData eventData = event.getData();

    if (!JobCallbackUtil.isThereJobCallbackProperty(jobRunner.getProps(),
        ON_COMPLETION_JOB_CALLBACK_STATUS)) {
      return;
    }

    // don't want to waste time resolving properties if there are no
    // callback properties to parse
    Props props = PropsUtils.resolveProps(jobRunner.getProps());

    Map<String, String> contextInfo =
        JobCallbackUtil.buildJobContextInfoMap(event, azkabanHostName);

    JobCallbackStatusEnum jobCallBackStatusEnum = null;
    Logger jobLogger = jobRunner.getLogger();

    Status jobStatus = eventData.getStatus();

    if (jobStatus == Status.SUCCEEDED) {

      jobCallBackStatusEnum = JobCallbackStatusEnum.SUCCESS;

    } else if (jobStatus == Status.FAILED
        || jobStatus == Status.FAILED_FINISHING || jobStatus == Status.KILLED) {

      jobCallBackStatusEnum = JobCallbackStatusEnum.FAILURE;
    } else {
      jobLogger.info("!!!! WE ARE NOT SUPPORTING JOB CALLBACKS FOR STATUS: "
          + jobStatus);
      jobCallBackStatusEnum = null; // to be explicit
    }

    String jobId = contextInfo.get(CONTEXT_JOB_TOKEN);

    if (jobCallBackStatusEnum != null) {
      List<HttpRequestBase> jobCallbackHttpRequests =
          JobCallbackUtil.parseJobCallbackProperties(props,
              jobCallBackStatusEnum, contextInfo, maxNumCallBack, jobLogger);

      if (!jobCallbackHttpRequests.isEmpty()) {
        String msg =
            String.format("Making %d job callbacks for status: %s",
                jobCallbackHttpRequests.size(), jobCallBackStatusEnum.name());
        jobLogger.info(msg);

        addDefaultHeaders(jobCallbackHttpRequests);

        JobCallbackRequestMaker.getInstance().makeHttpRequest(jobId, jobLogger,
            jobCallbackHttpRequests);
      } else {
        jobLogger.info("No job callbacks for status: " + jobCallBackStatusEnum);
      }
    }

    // for completed status
    List<HttpRequestBase> httpRequestsForCompletedStatus =
        JobCallbackUtil.parseJobCallbackProperties(props, COMPLETED,
            contextInfo, maxNumCallBack, jobLogger);

    // now make the call
    if (!httpRequestsForCompletedStatus.isEmpty()) {
      jobLogger.info("Making " + httpRequestsForCompletedStatus.size()
          + " job callbacks for status: " + COMPLETED);

      addDefaultHeaders(httpRequestsForCompletedStatus);
      JobCallbackRequestMaker.getInstance().makeHttpRequest(jobId, jobLogger,
          httpRequestsForCompletedStatus);
    } else {
      jobLogger.info("No job callbacks for status: " + COMPLETED);
    }
  }

  private void processJobCallOnStart(Event event) {
    JobRunner jobRunner = (JobRunner) event.getRunner();

    if (JobCallbackUtil.isThereJobCallbackProperty(jobRunner.getProps(),
        JobCallbackStatusEnum.STARTED)) {

      // don't want to waste time resolving properties if there are
      // callback properties to parse
      Props props = PropsUtils.resolveProps(jobRunner.getProps());

      Map<String, String> contextInfo =
          JobCallbackUtil.buildJobContextInfoMap(event, azkabanHostName);

      List<HttpRequestBase> jobCallbackHttpRequests =
          JobCallbackUtil.parseJobCallbackProperties(props, STARTED,
              contextInfo, maxNumCallBack, jobRunner.getLogger());

      String jobId = contextInfo.get(CONTEXT_JOB_TOKEN);
      String msg =
          String.format("Making %d job callbacks for job %s for jobStatus: %s",
              jobCallbackHttpRequests.size(), jobId, STARTED.name());

      jobRunner.getLogger().info(msg);

      addDefaultHeaders(jobCallbackHttpRequests);

      JobCallbackRequestMaker.getInstance().makeHttpRequest(jobId,
          jobRunner.getLogger(), jobCallbackHttpRequests);
    }
  }

  private String getAzkabanHostName(Props props) {
    String baseURL = props.get(JobRunner.AZKABAN_WEBSERVER_URL);
    try {
      String hostName = InetAddress.getLocalHost().getHostName();
      if (baseURL != null) {
        URL url = new URL(baseURL);
        hostName = url.getHost() + ":" + url.getPort();
      }
      return hostName;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Encountered while getting azkaban host name", e);
    }
  }

  private void addDefaultHeaders(List<HttpRequestBase> httpRequests) {
    if (httpRequests == null) {
      return;
    }

    SimpleDateFormat format =
        new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
    format.setTimeZone(TimeZone.getTimeZone("GMT"));

    for (HttpRequestBase httpRequest : httpRequests) {
      httpRequest.addHeader(new BasicHeader("Date", gmtDateFormatter
          .format(new Date())));
      httpRequest.addHeader(new BasicHeader("Host", azkabanHostName));
    }

  }
}
