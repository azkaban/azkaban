package azkaban.execapp.event;

import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_EXECUTION_ID_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_FLOW_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_STATUS_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_PROJECT_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_SERVER_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.FIRST_JOB_CALLBACK_URL_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.HEADER_ELEMENT_DELIMITER;
import static azkaban.jobcallback.JobCallbackConstants.HEADER_NAME_VALUE_DELIMITER;
import static azkaban.jobcallback.JobCallbackConstants.HTTP_GET;
import static azkaban.jobcallback.JobCallbackConstants.HTTP_POST;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_BODY_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_REQUEST_HEADERS_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_REQUEST_METHOD_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_URL_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.SEQUENCE_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.STATUS_TOKEN;

import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.jobcallback.JobCallbackStatusEnum;
import azkaban.utils.Props;
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

public class JobCallbackUtil {

  private static final Logger logger = Logger.getLogger(JobCallbackUtil.class);

  private static final Map<JobCallbackStatusEnum, String> firstJobcallbackPropertyMap =
      new HashMap<>(
          JobCallbackStatusEnum.values().length);

  static {
    for (final JobCallbackStatusEnum statusEnum : JobCallbackStatusEnum.values()) {
      firstJobcallbackPropertyMap.put(statusEnum,
          replaceStatusToken(FIRST_JOB_CALLBACK_URL_TEMPLATE, statusEnum));
    }
  }

  /**
   * Use to quickly determine if there is a job callback related property in the Props.
   *
   * @return true if there is job callback related property
   */
  public static boolean isThereJobCallbackProperty(final Props props,
      final JobCallbackStatusEnum status) {

    if (props == null || status == null) {
      throw new NullPointerException("One of the argument is null");
    }

    final String jobCallBackUrl = firstJobcallbackPropertyMap.get(status);
    return props.containsKey(jobCallBackUrl);
  }

  public static boolean isThereJobCallbackProperty(final Props props,
      final JobCallbackStatusEnum... jobStatuses) {

    if (props == null || jobStatuses == null) {
      throw new NullPointerException("One of the argument is null");
    }

    for (final JobCallbackStatusEnum jobStatus : jobStatuses) {
      if (JobCallbackUtil.isThereJobCallbackProperty(props, jobStatus)) {
        return true;
      }
    }
    return false;
  }

  public static List<HttpRequestBase> parseJobCallbackProperties(final Props props,
      final JobCallbackStatusEnum status, final Map<String, String> contextInfo,
      final int maxNumCallback) {

    return parseJobCallbackProperties(props, status, contextInfo,
        maxNumCallback, logger);
  }

  /**
   * This method is responsible for parsing job call URL properties and convert them into a list of
   * HttpRequestBase, which callers can use to execute.
   *
   * In addition to parsing, it will also replace the tokens with actual values.
   *
   * @return List<HttpRequestBase> - empty if no job callback related properties
   */
  public static List<HttpRequestBase> parseJobCallbackProperties(final Props props,
      final JobCallbackStatusEnum status, final Map<String, String> contextInfo,
      final int maxNumCallback, final Logger privateLogger) {
    String callbackUrl = null;

    if (!isThereJobCallbackProperty(props, status)) {
      // short circuit
      return Collections.emptyList();
    }

    final List<HttpRequestBase> result = new ArrayList<>();

    // replace property templates with status
    final String jobCallBackUrlKey =
        replaceStatusToken(JOB_CALLBACK_URL_TEMPLATE, status);

    final String requestMethod =
        replaceStatusToken(JOB_CALLBACK_REQUEST_METHOD_TEMPLATE, status);

    final String httpBodyKey = replaceStatusToken(JOB_CALLBACK_BODY_TEMPLATE, status);

    final String headersKey =
        replaceStatusToken(JOB_CALLBACK_REQUEST_HEADERS_TEMPLATE, status);

    for (int sequence = 1; sequence <= maxNumCallback; sequence++) {
      HttpRequestBase httpRequest = null;
      final String sequenceStr = Integer.toString(sequence);
      // callback url
      final String callbackUrlKey =
          jobCallBackUrlKey.replace(SEQUENCE_TOKEN, sequenceStr);

      callbackUrl = props.get(callbackUrlKey);
      if (callbackUrl == null || callbackUrl.length() == 0) {
        // no more needs to done
        break;
      } else {
        final String callbackUrlWithTokenReplaced =
            replaceTokens(callbackUrl, contextInfo, true);

        final String requestMethodKey =
            requestMethod.replace(SEQUENCE_TOKEN, sequenceStr);

        final String method = props.getString(requestMethodKey, HTTP_GET);

        if (HTTP_POST.equals(method)) {
          final String postBodyKey = httpBodyKey.replace(SEQUENCE_TOKEN, sequenceStr);
          final String httpBodyValue = props.get(postBodyKey);
          if (httpBodyValue == null) {
            // missing body for POST, not good
            // update the wiki about skipping callback url if body is missing
            privateLogger.warn("Missing value for key: " + postBodyKey
                + " skipping job callback '" + callbackUrl + " for job "
                + contextInfo.get(CONTEXT_JOB_TOKEN));
          } else {
            // put together an URL
            privateLogger.info("callbackUrlWithTokenReplaced: " + callbackUrlWithTokenReplaced);
            final HttpPost httpPost = new HttpPost(callbackUrlWithTokenReplaced);
            final String postActualBody =
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

        final String headersKeyPerSequence =
            headersKey.replace(SEQUENCE_TOKEN, sequenceStr);
        final String headersValue = props.get(headersKeyPerSequence);
        privateLogger.info("headers: " + headersValue);
        final Header[] headers = parseHttpHeaders(headersValue);
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
   * @return null if headers is null or empty
   */
  public static Header[] parseHttpHeaders(final String headers) {
    if (headers == null || headers.length() == 0) {
      return null;
    }

    final String[] headerArray = headers.split(HEADER_ELEMENT_DELIMITER);
    final List<Header> headerList = new ArrayList<>(headerArray.length);
    for (int i = 0; i < headerArray.length; i++) {
      final String headerPair = headerArray[i];
      final int index = headerPair.indexOf(HEADER_NAME_VALUE_DELIMITER);
      if (index != -1) {
        headerList.add(new BasicHeader(headerPair.substring(0, index),
            headerPair.substring(index + 1)));
      }

    }
    return headerList.toArray(new BasicHeader[0]);
  }

  private static String replaceStatusToken(final String template,
      final JobCallbackStatusEnum status) {
    return template.replaceAll(STATUS_TOKEN, status.name().toLowerCase());
  }

  private static StringEntity createStringEntity(final String str) {
    try {
      return new StringEntity(str);
    } catch (final UnsupportedEncodingException e) {
      throw new RuntimeException("Encoding not supported", e);
    }
  }

  /**
   * This method takes the job context info. and put the values into a map with keys as the tokens.
   *
   * @return Map<String,String>
   */
  public static Map<String, String> buildJobContextInfoMap(final Event event, final String server) {
    final EventData eventData = event.getData();
    final String projectName = eventData.getProjectName();
    final String flowName = eventData.getFlowName();
    final String executionId =
        String.valueOf(eventData.getExecutionId());
    final String jobId = eventData.getJobId();

    final Map<String, String> result = new HashMap<>();
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
  }

  /**
   * Replace the supported tokens in the URL with values in the contextInfo. This will also make
   * sure the values are HTTP encoded.
   *
   * @param withEncoding - whether the token values will be HTTP encoded
   * @return String - value with tokens replaced with values
   */
  public static String replaceTokens(final String value,
      final Map<String, String> contextInfo, final boolean withEncoding) {

    String result = value;
    String tokenValue =
        encodeQueryParam(contextInfo.get(CONTEXT_SERVER_TOKEN), withEncoding);
    result = result.replaceAll(Pattern.quote(CONTEXT_SERVER_TOKEN), tokenValue);

    tokenValue = encodeQueryParam(contextInfo.get(CONTEXT_PROJECT_TOKEN), withEncoding);
    result = result.replaceAll(Pattern.quote(CONTEXT_PROJECT_TOKEN), tokenValue);

    tokenValue = encodeQueryParam(contextInfo.get(CONTEXT_FLOW_TOKEN), withEncoding);
    result = result.replaceAll(Pattern.quote(CONTEXT_FLOW_TOKEN), tokenValue);

    tokenValue = encodeQueryParam(contextInfo.get(CONTEXT_JOB_TOKEN), withEncoding);
    result = result.replaceAll(Pattern.quote(CONTEXT_JOB_TOKEN), tokenValue);

    tokenValue =
        encodeQueryParam(contextInfo.get(CONTEXT_EXECUTION_ID_TOKEN), withEncoding);
    result = result.replaceAll(Pattern.quote(CONTEXT_EXECUTION_ID_TOKEN), tokenValue);

    tokenValue =
        encodeQueryParam(contextInfo.get(CONTEXT_JOB_STATUS_TOKEN), withEncoding);

    result = result.replaceAll(Pattern.quote(CONTEXT_JOB_STATUS_TOKEN), tokenValue);

    return result;
  }

  private static String encodeQueryParam(final String str, final boolean withEncoding) {
    if (!withEncoding) {
      return str;
    }
    try {
      return URLEncoder.encode(str, "UTF-8");
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalArgumentException(
          "Encountered problem during encoding:", e);
    }
  }
}
