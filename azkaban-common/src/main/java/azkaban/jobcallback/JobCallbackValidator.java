package azkaban.jobcallback;

import static azkaban.jobcallback.JobCallbackConstants.DEFAULT_POST_BODY_LENGTH;
import static azkaban.jobcallback.JobCallbackConstants.HTTP_GET;
import static azkaban.jobcallback.JobCallbackConstants.HTTP_POST;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_BODY_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_REQUEST_METHOD_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.JOB_CALLBACK_URL_TEMPLATE;
import static azkaban.jobcallback.JobCallbackConstants.MAX_POST_BODY_LENGTH_PROPERTY_KEY;
import static azkaban.jobcallback.JobCallbackConstants.SEQUENCE_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.STATUS_TOKEN;

import azkaban.utils.Props;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responsible for validating the job callback related properties at project upload time
 *
 * @author hluu
 */
public class JobCallbackValidator {

  private static final Logger LOG = LoggerFactory.getLogger(JobCallbackValidator.class);

  /**
   * Make sure all the job callback related properties are valid
   *
   * @return number of valid job callback properties. Mainly for testing purpose.
   */
  public static int validate(final String jobName, final Props serverProps, final Props jobProps,
      final Collection<String> errors) {
    final int maxNumCallback =
        serverProps.getInt(
            JobCallbackConstants.MAX_CALLBACK_COUNT_PROPERTY_KEY,
            JobCallbackConstants.DEFAULT_MAX_CALLBACK_COUNT);

    final int maxPostBodyLength =
        serverProps.getInt(MAX_POST_BODY_LENGTH_PROPERTY_KEY,
            DEFAULT_POST_BODY_LENGTH);

    int totalCallbackCount = 0;
    for (final JobCallbackStatusEnum jobStatus : JobCallbackStatusEnum.values()) {
      totalCallbackCount +=
          validateBasedOnStatus(jobProps, errors, jobStatus, maxNumCallback,
              maxPostBodyLength);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Found " + totalCallbackCount + " job callbacks for job "
          + jobName);
    }
    return totalCallbackCount;
  }

  private static int validateBasedOnStatus(final Props jobProps,
      final Collection<String> errors, final JobCallbackStatusEnum jobStatus,
      final int maxNumCallback, final int maxPostBodyLength) {

    int callbackCount = 0;
    // replace property templates with status
    final String jobCallBackUrl =
        JOB_CALLBACK_URL_TEMPLATE.replaceFirst(STATUS_TOKEN, jobStatus.name()
            .toLowerCase());

    final String requestMethod =
        JOB_CALLBACK_REQUEST_METHOD_TEMPLATE.replaceFirst(STATUS_TOKEN,
            jobStatus.name().toLowerCase());

    final String httpBody =
        JOB_CALLBACK_BODY_TEMPLATE.replaceFirst(STATUS_TOKEN, jobStatus.name()
            .toLowerCase());

    for (int i = 0; i <= maxNumCallback; i++) {
      // callback url
      final String callbackUrlKey =
          jobCallBackUrl.replaceFirst(SEQUENCE_TOKEN, Integer.toString(i));
      final String callbackUrlValue = jobProps.get(callbackUrlKey);

      // sequence number should start at 1, this is to check for sequence
      // number that starts a 0
      if (i == 0) {
        if (callbackUrlValue != null) {
          errors.add("Sequence number starts at 1, not 0");
        }
        continue;
      }

      if (callbackUrlValue == null || callbackUrlValue.length() == 0) {
        break;
      } else {
        final String requestMethodKey =
            requestMethod.replaceFirst(SEQUENCE_TOKEN, Integer.toString(i));

        final String methodValue = jobProps.getString(requestMethodKey, HTTP_GET);

        if (HTTP_POST.equals(methodValue)) {
          // now try to get the post body
          final String postBodyKey =
              httpBody.replaceFirst(SEQUENCE_TOKEN, Integer.toString(i));
          final String postBodyValue = jobProps.get(postBodyKey);
          if (postBodyValue == null || postBodyValue.length() == 0) {
            errors.add("No POST body was specified for job callback '"
                + callbackUrlValue + "'");
          } else if (postBodyValue.length() > maxPostBodyLength) {
            errors.add("POST body length is : " + postBodyValue.length()
                + " which is larger than supported length of "
                + maxPostBodyLength);
          } else {
            callbackCount++;
          }
        } else if (HTTP_GET.equals(methodValue)) {
          // that's cool
          callbackCount++;
        } else {
          errors.add("Unsupported request method: " + methodValue
              + " Only POST and GET are supported");
        }
      }
    }

    return callbackCount;
  }
}
