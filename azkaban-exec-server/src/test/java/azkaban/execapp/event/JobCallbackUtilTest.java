package azkaban.execapp.event;

import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_EXECUTION_ID_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_FLOW_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_STATUS_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_JOB_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_PROJECT_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.CONTEXT_SERVER_TOKEN;
import static azkaban.jobcallback.JobCallbackConstants.HTTP_GET;
import static azkaban.jobcallback.JobCallbackConstants.HTTP_POST;

import azkaban.jobcallback.JobCallbackConstants;
import azkaban.jobcallback.JobCallbackStatusEnum;
import azkaban.utils.Props;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobCallbackUtilTest {

  private static final String SERVER_NAME = "localhost:9999";
  private static final String PROJECT_NAME = "PROJECTX";
  private static final String FLOW_NAME = "FLOWX";
  private static final String JOB_NAME = "JOBX";
  private static final String EXECUTION_ID = "1234";
  private static final String JOB_STATUS_NAME = JobCallbackStatusEnum.STARTED
      .name();
  private static Map<String, String> contextInfo;

  @BeforeClass
  public static void setup() {
    contextInfo = new HashMap<>();
    contextInfo.put(CONTEXT_SERVER_TOKEN, SERVER_NAME);
    contextInfo.put(CONTEXT_PROJECT_TOKEN, PROJECT_NAME);
    contextInfo.put(CONTEXT_FLOW_TOKEN, FLOW_NAME);
    contextInfo.put(CONTEXT_EXECUTION_ID_TOKEN, EXECUTION_ID);
    contextInfo.put(CONTEXT_JOB_TOKEN, JOB_NAME);
    contextInfo.put(CONTEXT_JOB_STATUS_TOKEN, JOB_STATUS_NAME);
  }

  @Test
  public void noCallbackPropertiesTest() {
    final Props props = new Props();
    props.put("abc", "def");

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.STARTED));

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.COMPLETED));

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.FAILURE));

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.SUCCESS));
  }

  @Test
  public void hasCallbackPropertiesTest() {
    final Props props = new Props();
    for (final JobCallbackStatusEnum jobStatus : JobCallbackStatusEnum.values()) {
      props.put(
          "job.notification." + jobStatus.name().toLowerCase() + ".1.url",
          "def");
    }

    System.out.println(props);

    Assert.assertTrue(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.STARTED));

    Assert.assertTrue(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.COMPLETED));

    Assert.assertTrue(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.FAILURE));

    Assert.assertTrue(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.SUCCESS));
  }

  @Test
  public void multipleStatusWithNoJobCallbackTest() {
    final Props props = new Props();
    props.put("abc", "def");

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.STARTED, JobCallbackStatusEnum.COMPLETED,
        JobCallbackStatusEnum.FAILURE, JobCallbackStatusEnum.SUCCESS));

  }

  @Test
  public void multipleStatusesWithJobCallbackTest() {
    Props props = new Props();
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", "def");

    Assert.assertTrue(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.STARTED, JobCallbackStatusEnum.COMPLETED,
        JobCallbackStatusEnum.FAILURE, JobCallbackStatusEnum.SUCCESS));

    props = new Props();
    props.put("job.notification."
            + JobCallbackStatusEnum.COMPLETED.name().toLowerCase() + ".1.url",
        "def");
    Assert.assertTrue(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.STARTED, JobCallbackStatusEnum.COMPLETED,
        JobCallbackStatusEnum.FAILURE, JobCallbackStatusEnum.SUCCESS));

    props = new Props();
    props.put("job.notification."
        + JobCallbackStatusEnum.FAILURE.name().toLowerCase() + ".1.url", "def");
    Assert.assertTrue(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.STARTED, JobCallbackStatusEnum.COMPLETED,
        JobCallbackStatusEnum.FAILURE, JobCallbackStatusEnum.SUCCESS));

    props = new Props();
    props.put("job.notification."
        + JobCallbackStatusEnum.SUCCESS.name().toLowerCase() + ".1.url", "def");
    Assert.assertTrue(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.STARTED, JobCallbackStatusEnum.COMPLETED,
        JobCallbackStatusEnum.FAILURE, JobCallbackStatusEnum.SUCCESS));
  }

  @Test
  public void hasCallbackPropertiesWithGapTest() {
    final Props props = new Props();
    for (final JobCallbackStatusEnum jobStatus : JobCallbackStatusEnum.values()) {
      props.put(
          "job.notification." + jobStatus.name().toLowerCase() + ".2.url",
          "def");
    }

    System.out.println(props);

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.STARTED));

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.COMPLETED));

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.FAILURE));

    Assert.assertFalse(JobCallbackUtil.isThereJobCallbackProperty(props,
        JobCallbackStatusEnum.SUCCESS));
  }

  @Test
  public void noTokenTest() {
    final String urlWithNoToken = "http://www.linkedin.com";
    final String result =
        JobCallbackUtil.replaceTokens(urlWithNoToken, contextInfo, true);
    Assert.assertEquals(urlWithNoToken, result);
  }

  @Test
  public void oneTokenTest() {

    final String urlWithOneToken =
        "http://www.linkedin.com?project=" + CONTEXT_PROJECT_TOKEN + "&another=yes";

    final String result =
        JobCallbackUtil.replaceTokens(urlWithOneToken, contextInfo, true);
    Assert.assertEquals("http://www.linkedin.com?project=" + PROJECT_NAME
        + "&another=yes", result);
  }

  @Test
  public void twoTokensTest() {

    final String urlWithOneToken =
        "http://www.linkedin.com?project=" + CONTEXT_PROJECT_TOKEN + "&flow="
            + CONTEXT_FLOW_TOKEN;

    final String result =
        JobCallbackUtil.replaceTokens(urlWithOneToken, contextInfo, true);
    Assert.assertEquals("http://www.linkedin.com?project=" + PROJECT_NAME
        + "&flow=" + FLOW_NAME, result);
  }

  @Test
  public void allTokensTest() {

    final String urlWithOneToken =
        "http://www.linkedin.com?server=" + SERVER_NAME + "&project="
            + CONTEXT_PROJECT_TOKEN + "&flow=" + CONTEXT_FLOW_TOKEN + "&executionId="
            + CONTEXT_EXECUTION_ID_TOKEN + "&job=" + CONTEXT_JOB_TOKEN + "&status="
            + CONTEXT_JOB_STATUS_TOKEN;

    final String result =
        JobCallbackUtil.replaceTokens(urlWithOneToken, contextInfo, true);

    final String expectedResult =
        "http://www.linkedin.com?server=" + SERVER_NAME + "&project="
            + PROJECT_NAME + "&flow=" + FLOW_NAME + "&executionId="
            + EXECUTION_ID + "&job=" + JOB_NAME + "&status=" + JOB_STATUS_NAME;

    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public void tokenWithEncoding() throws Exception {
    final String jobNameWithSpaces = "my job";
    final String encodedJobName = URLEncoder.encode(jobNameWithSpaces, "UTF-8");

    Map<String, String> customContextInfo = new HashMap<>();
    customContextInfo = new HashMap<>();
    customContextInfo.put(CONTEXT_SERVER_TOKEN, SERVER_NAME);
    customContextInfo.put(CONTEXT_PROJECT_TOKEN, PROJECT_NAME);
    customContextInfo.put(CONTEXT_FLOW_TOKEN, FLOW_NAME);
    customContextInfo.put(CONTEXT_EXECUTION_ID_TOKEN, EXECUTION_ID);
    customContextInfo.put(CONTEXT_JOB_TOKEN, jobNameWithSpaces);
    customContextInfo.put(CONTEXT_JOB_STATUS_TOKEN, JOB_STATUS_NAME);

    final String urlWithOneToken =
        "http://www.linkedin.com?job=" + CONTEXT_JOB_TOKEN + "&flow=" + CONTEXT_FLOW_TOKEN;

    final String result =
        JobCallbackUtil.replaceTokens(urlWithOneToken, customContextInfo, true);
    Assert.assertEquals("http://www.linkedin.com?job=" + encodedJobName
        + "&flow=" + FLOW_NAME, result);
  }

  @Test
  public void parseJobCallbackOneGetTest() {
    final Props props = new Props();
    final String url = "http://lva1-rpt07.corp.linkedin.com";
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);
    final List<HttpRequestBase> result =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    Assert.assertEquals(1, result.size());
    Assert.assertEquals(HTTP_GET, result.get(0).getMethod());
    Assert.assertEquals(url, result.get(0).getURI().toString());
  }

  @Test
  public void parseJobCallbackWithInvalidURLTest() {
    final Props props = new Props();
    final String url = "linkedin.com";
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);
    final List<HttpRequestBase> result =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    Assert.assertEquals(1, result.size());
    Assert.assertEquals(HTTP_GET, result.get(0).getMethod());
    Assert.assertEquals(url, result.get(0).getURI().toString());
  }

  @Test
  public void parseJobCallbackTwoGetsTest() {
    final Props props = new Props();
    final String[] urls =
        {"http://lva1-rpt07.corp.linkedin.com",
            "http://lva1-rpt06.corp.linkedin.com"};
    props.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url",
        urls[0]);
    props.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".2.url",
        urls[1]);
    final List<HttpRequestBase> result =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    Assert.assertEquals(2, result.size());
    for (int i = 0; i < urls.length; i++) {
      Assert.assertEquals(HTTP_GET, result.get(i).getMethod());
      Assert.assertEquals(urls[i], result.get(i).getURI().toString());
    }
  }

  @Test
  public void parseJobCallbackWithGapTest() {
    final Props props = new Props();
    final String[] urls =
        {"http://lva1-rpt07.corp.linkedin.com",
            "http://lva1-rpt06.corp.linkedin.com"};
    props.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url",
        urls[0]);
    props.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".3.url",
        urls[1]);
    final List<HttpRequestBase> result =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    Assert.assertEquals(1, result.size());
    Assert.assertEquals(HTTP_GET, result.get(0).getMethod());
    Assert.assertEquals(urls[0], result.get(0).getURI().toString());
  }

  @Test
  public void parseJobCallbackWithPostTest() {
    final Props props = new Props();
    final String url = "http://lva1-rpt07.corp.linkedin.com";
    final String bodyText = "{name:\"you\"}";
    props.put("job.notification."
        + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.url", url);
    props.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.method",
        HTTP_POST);

    props.put("job.notification."
            + JobCallbackStatusEnum.STARTED.name().toLowerCase() + ".1.body",
        bodyText);

    final List<HttpRequestBase> result =
        JobCallbackUtil.parseJobCallbackProperties(props,
            JobCallbackStatusEnum.STARTED, contextInfo, 3);

    Assert.assertEquals(1, result.size());

    final HttpPost httpPost = (HttpPost) result.get(0);

    Assert.assertEquals(url, httpPost.getURI().toString());
    Assert.assertEquals(HTTP_POST, httpPost.getMethod());

    Assert.assertEquals(bodyText.length(), httpPost.getEntity()
        .getContentLength());

  }

  @Test
  public void noHeaderElementTest() {
    final Header[] headerArr =
        JobCallbackUtil.parseHttpHeaders("this is an amazing day");

    Assert.assertNotNull(headerArr);
    Assert.assertEquals(0, headerArr.length);
  }

  @Test
  public void oneHeaderElementTest() {
    final String name = "Content-type";
    final String value = "application/json";
    final String headers =
        name + JobCallbackConstants.HEADER_NAME_VALUE_DELIMITER + value;
    Header[] headerArr = JobCallbackUtil.parseHttpHeaders(headers);

    Assert.assertNotNull(headerArr);
    Assert.assertEquals(1, headerArr.length);
    Assert.assertEquals(name, headerArr[0].getName());
    Assert.assertEquals(value, headerArr[0].getValue());

    final String headersWithExtraDelimiter =
        name + JobCallbackConstants.HEADER_NAME_VALUE_DELIMITER + value
            + JobCallbackConstants.HEADER_ELEMENT_DELIMITER;

    headerArr = JobCallbackUtil.parseHttpHeaders(headersWithExtraDelimiter);
    Assert.assertNotNull(headerArr);
    Assert.assertEquals(1, headerArr.length);
    Assert.assertEquals(name, headerArr[0].getName());
    Assert.assertEquals(value, headerArr[0].getValue());

  }

  @Test
  public void multipleHeaderElementTest() {
    final String name1 = "Content-type";
    final String value1 = "application/json";

    final String name2 = "Accept";
    final String value2 = "application/xml";

    final String name3 = "User-Agent";
    final String value3 =
        "Mozilla/5.0 (X11; Linux x86_64; rv:12.0) Gecko/20100101 Firefox/21.0";

    String headers = makeHeaderElement(name1, value1);
    headers += JobCallbackConstants.HEADER_ELEMENT_DELIMITER;
    headers += makeHeaderElement(name2, value2);
    headers += JobCallbackConstants.HEADER_ELEMENT_DELIMITER;
    headers += makeHeaderElement(name3, value3);

    System.out.println("headers: " + headers);
    final Header[] headerArr = JobCallbackUtil.parseHttpHeaders(headers);

    Assert.assertNotNull(headerArr);
    Assert.assertEquals(3, headerArr.length);
    Assert.assertEquals(name1, headerArr[0].getName());
    Assert.assertEquals(value1, headerArr[0].getValue());
    Assert.assertEquals(name2, headerArr[1].getName());
    Assert.assertEquals(value2, headerArr[1].getValue());
    Assert.assertEquals(name3, headerArr[2].getName());
    Assert.assertEquals(value3, headerArr[2].getValue());
  }

  @Test
  public void partialHeaderElementTest() {
    final String name1 = "Content-type";
    final String value1 = "application/json";

    final String name2 = "Accept";
    final String value2 = "";

    final String name3 = "User-Agent";
    final String value3 =
        "Mozilla/5.0 (X11; Linux x86_64; rv:12.0) Gecko/20100101 Firefox/21.0";

    String headers = makeHeaderElement(name1, value1);
    headers += JobCallbackConstants.HEADER_ELEMENT_DELIMITER;
    headers += makeHeaderElement(name2, value2);
    headers += JobCallbackConstants.HEADER_ELEMENT_DELIMITER;
    headers += makeHeaderElement(name3, value3);

    System.out.println("headers: " + headers);
    final Header[] headerArr = JobCallbackUtil.parseHttpHeaders(headers);

    Assert.assertNotNull(headerArr);
    Assert.assertEquals(3, headerArr.length);
    Assert.assertEquals(name1, headerArr[0].getName());
    Assert.assertEquals(value1, headerArr[0].getValue());
    Assert.assertEquals(name2, headerArr[1].getName());
    Assert.assertEquals(value2, headerArr[1].getValue());
    Assert.assertEquals(name3, headerArr[2].getName());
    Assert.assertEquals(value3, headerArr[2].getValue());
  }

  private String makeHeaderElement(final String name, final String value) {
    return name + JobCallbackConstants.HEADER_NAME_VALUE_DELIMITER + value;
  }

}
