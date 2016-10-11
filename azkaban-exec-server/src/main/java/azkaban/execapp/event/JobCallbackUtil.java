package azkaban.execapp.event;

import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_EXECUTION_ID_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.FIRST_JOB_CALLBACK_URL_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_FLOW_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.HEADER_ELEMENT_DELIMITER;
import static azkaban.jobcallback.JobCallbackConstants.HEADER_NAME_VALUE_DELIMITER;
import static azkaban.jobcallback.JobCallbackConstants.HTTP_GET;
import static azkaban.jobcallback.JobCallbackConstants.HTTP_POST;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_BODY_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_REQUEST_HEADERS_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_REQUEST_METHOD_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_URL_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_STATUS_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_PROJECT_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.SEQUENCE_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_SERVER_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.STATUS_TOKEN;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.log4j.Logger;

import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.execapp.JobRunner;
import azkaban.executor.ExecutableNode;
import azkaban.jobcallback.JobCallbackStatusEnum;
import azkaban.utils.Props;

public class JobCallbackUtil {
  private static final Logger logger = Logger.getLogger(JobCallbackUtil.class);

  private static Map<JobCallbackStatusEnum, String> firstJobcallbackPropertyMap =
      new HashMap<JobCallbackStatusEnum, String>(
          JobCallbackStatusEnum.values().length);

  static {
    for (JobCallbackStatusEnum statusEnum : JobCallbackStatusEnum.values()) {
      firstJobcallbackPropertyMap.put(statusEnum,
          replaceStatusToken(FIRST_JOB_CALLBACK_URL_TEMPLATE, statusEnum));
    }
  }

  /**
   * Use to quickly determine if there is a job callback related property in the
   * Props.
   * 
   * @param props
   * @param status
   * @return true if there is job callback related property
   */
  public static boolean isThereJobCallbackProperty(Props props,
      JobCallbackStatusEnum status) {

    if (props == null || status == null) {
      throw new NullPointerException("One of the argument is null");
    }

    String jobCallBackUrl = firstJobcallbackPropertyMap.get(status);
    return props.containsKey(jobCallBackUrl);
  }

  public static boolean isThereJobCallbackProperty(Props props,
      JobCallbackStatusEnum... jobStatuses) {

    if (props == null || jobStatuses == null) {
      throw new NullPointerException("One of the argument is null");
    }

    for (JobCallbackStatusEnum jobStatus : jobStatuses) {
      if (JobCallbackUtil.isThereJobCallbackProperty(props, jobStatus)) {
        return true;
      }
    }
    return false;
  }

  public static List<HttpRequestBase> parseJobCallbackProperties(Props props,
      JobCallbackStatusEnum status, Map<String, String> contextInfo,
      int maxNumCallback) {

    return parseJobCallbackProperties(props, status, contextInfo,
        maxNumCallback, logger);
  }

  /**
   * This method is responsible for parsing job call URL properties and convert
   * them into a list of HttpRequestBase, which callers can use to execute.
   * 
   * In addition to parsing, it will also replace the tokens with actual values.
   * 
   * @param props
   * @param status
   * @param event
   * @return List<HttpRequestBase> - empty if no job callback related properties
   */
  public static List<HttpRequestBase> parseJobCallbackProperties(Props props,
      JobCallbackStatusEnum status, Map<String, String> contextInfo,
      int maxNumCallback, Logger privateLogger) {
    String callbackUrl = null;

    if (!isThereJobCallbackProperty(props, status)) {
      // short circuit
      return Collections.emptyList();
    }

    List<HttpRequestBase> result = new ArrayList<HttpRequestBase>();

    // replace property templates with status
    String jobCallBackUrlKey =
        replaceStatusToken(JOB_CALLBACK_URL_TEMPLATE, status);

    String requestMethod =
        replaceStatusToken(JOB_CALLBACK_REQUEST_METHOD_TEMPLATE, status);

    String httpBodyKey = replaceStatusToken(JOB_CALLBACK_BODY_TEMPLATE, status);

    String headersKey =
        replaceStatusToken(JOB_CALLBACK_REQUEST_HEADERS_TEMPLATE, status);

    for (int sequence = 1; sequence <= maxNumCallback; sequence++) {
      HttpRequestBase httpRequest = null;
      String sequenceStr = Integer.toString(sequence);
      // callback url
      String callbackUrlKey =
          jobCallBackUrlKey.replace(SEQUENCE_TOKEN, sequenceStr);

      callbackUrl = props.get(callbackUrlKey);
      if (callbackUrl == null || callbackUrl.length() == 0) {
        // no more needs to done
        break;
      } else {
        String callbackUrlWithTokenReplaced =
            replaceTokens(callbackUrl, contextInfo, true);

        String requestMethodKey =
            requestMethod.replace(SEQUENCE_TOKEN, sequenceStr);

        String method = props.getString(requestMethodKey, HTTP_GET);

        if (HTTP_POST.equals(method)) {
          String postBodyKey = httpBodyKey.replace(SEQUENCE_TOKEN, sequenceStr);
          String httpBodyValue = props.get(postBodyKey);
          if (httpBodyValue == null) {
            // missing body for POST, not good
            // update the wiki about skipping callback url if body is missing
            privateLogger.warn("Missing value for key: " + postBodyKey
                + " skipping job callback '" + callbackUrl + " for job "
                + contextInfo.get(CONTEXT_JOB_TOKEN));
          } else {
            // put together an URL
            HttpPost httpPost = new HttpPost(callbackUrlWithTokenReplaced);
            String postActualBody =
                replaceTokens(httpBodyValue, contextInfo, false);
            privateLogger.info("postActualBody: " + postActualBody);
            httpPost.setEntity(createStringEntity(postActualBody));
            httpRequest = httpPost;
          }
        } else if (HTTP_GET.equals(method)) {
          // GET
          httpRequest = new HttpGet(callbackUrlWithTokenReplaced);
        } else {
          privateLogger.warn("Unsupported request method: " + method
              + ". Only POST and GET are supported");
        }

        String headersKeyPerSequence =
            headersKey.replace(SEQUENCE_TOKEN, sequenceStr);
        String headersValue = props.get(headersKeyPerSequence);
        privateLogger.info("headers: " + headersValue);
        Header[] headers = parseHttpHeaders(headersValue);
        if (headers != null) {
          httpRequest.setHeaders(headers);
          privateLogger.info("# of headers found: " + headers.length);
        }
        result.add(httpRequest);
      }
    }
    return result;
  }

  /**
   * Parse headers
   * 
   * @param headers
   * @return null if headers is null or empty
   */
  public static Header[] parseHttpHeaders(String headers) {
    if (headers == null || headers.length() == 0) {
      return null;
    }

    String[] headerArray = headers.split(HEADER_ELEMENT_DELIMITER);
    List<Header> headerList = new ArrayList<Header>(headerArray.length);
    for (int i = 0; i < headerArray.length; i++) {
      String headerPair = headerArray[i];
      int index = headerPair.indexOf(HEADER_NAME_VALUE_DELIMITER);
      if (index != -1) {
        headerList.add(new BasicHeader(headerPair.substring(0, index),
            headerPair.substring(index + 1)));
      }

    }
    return headerList.toArray(new BasicHeader[0]);
  }

  private static String replaceStatusToken(String template,
      JobCallbackStatusEnum status) {
    return template.replaceFirst(STATUS_TOKEN, status.name().toLowerCase());
  }

  private static StringEntity createStringEntity(String str) {
    try {
      return new StringEntity(str);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Encoding not supported", e);
    }
  }

  /**
   * This method takes the job context info. and put the values into a map with
   * keys as the tokens.
   * 
   * @param event
   * @return Map<String,String>
   */
  public static Map<String, String> buildJobContextInfoMap(Event event,
      String server) {

    if (event.getRunner() instanceof JobRunner) {
      JobRunner jobRunner = (JobRunner) event.getRunner();
      ExecutableNode node = jobRunner.getNode();
      EventData eventData = event.getData();
      String projectName = node.getParentFlow().getProjectName();
      String flowName = node.getParentFlow().getFlowId();
      String executionId =
          String.valueOf(node.getParentFlow().getExecutionId());
      String jobId = node.getId();

      Map<String, String> result = new HashMap<String, String>();
      result.put(CONTEXT_SERVER_TOKEN, server);
      result.put(CONTEXT_PROJECT_TOKEN, projectName);
      result.put(CONTEXT_FLOW_TOKEN, flowName);
      result.put(CONTEXT_EXECUTION_ID_TOKEN, executionId);
      result.put(CONTEXT_JOB_TOKEN, jobId);
      result.put(CONTEXT_JOB_STATUS_TOKEN, eventData.getStatus().name().toLowerCase());

      /*
       * if (node.getStatus() == Status.SUCCEEDED || node.getStatus() ==
       * Status.FAILED) { result.put(JOB_STATUS_TOKEN,
       * node.getStatus().name().toLowerCase()); } else if (node.getStatus() ==
       * Status.PREPARING) { result.put(JOB_STATUS_TOKEN, "started"); }
       */

      return result;

    } else {
      throw new IllegalArgumentException("Provided event is not a job event");
    }
  }

  /**
   * Replace the supported tokens in the URL with values in the contextInfo.
   * This will also make sure the values are HTTP encoded.
   * 
   * @param value
   * @param contextInfo
   * @param withEncoding - whether the token values will be HTTP encoded
   * @return String - value with tokens replaced with values
   */
  public static String replaceTokens(String value,
      Map<String, String> contextInfo, boolean withEncoding) {

    String result = value;
    String tokenValue =
        encodeQueryParam(contextInfo.get(CONTEXT_SERVER_TOKEN), withEncoding);
    result = result.replaceFirst(Pattern.quote(CONTEXT_SERVER_TOKEN), tokenValue);

    tokenValue = encodeQueryParam(contextInfo.get(CONTEXT_PROJECT_TOKEN), withEncoding);
    result = result.replaceFirst(Pattern.quote(CONTEXT_PROJECT_TOKEN), tokenValue);

    tokenValue = encodeQueryParam(contextInfo.get(CONTEXT_FLOW_TOKEN), withEncoding);
    result = result.replaceFirst(Pattern.quote(CONTEXT_FLOW_TOKEN), tokenValue);

    tokenValue = encodeQueryParam(contextInfo.get(CONTEXT_JOB_TOKEN), withEncoding);
    result = result.replaceFirst(Pattern.quote(CONTEXT_JOB_TOKEN), tokenValue);

    tokenValue =
        encodeQueryParam(contextInfo.get(CONTEXT_EXECUTION_ID_TOKEN), withEncoding);
    result = result.replaceFirst(Pattern.quote(CONTEXT_EXECUTION_ID_TOKEN), tokenValue);

    tokenValue =
        encodeQueryParam(contextInfo.get(CONTEXT_JOB_STATUS_TOKEN), withEncoding);

    result = result.replaceFirst(Pattern.quote(CONTEXT_JOB_STATUS_TOKEN), tokenValue);

    return result;
  }

  private static String encodeQueryParam(String str, boolean withEncoding) {
    if (!withEncoding) {
      return str;
    }
    try {
      return URLEncoder.encode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(
          "Encountered problem during encoding:", e);
    }
  }
}
