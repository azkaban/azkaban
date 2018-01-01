package azkaban.executor.mail;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.utils.EmailMessage;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Test;

public class MailCreatorsTest {

  // 2016/07/17 08:54:10 UTC
  public static final long SUBMIT_TIME_MILLIS = 1468745650608L;
  // 2016/07/17 08:54:11 UTC
  public static final long START_TIME_MILLIS = 1468745651608L;
  // 2016/07/17 08:54:16 UTC (START_TIME_MILLIS + 5 seconds)
  public static final long END_TIME_MILLIS = START_TIME_MILLIS + 5_000L;
  // 2016/07/17 08:54:21 UTC (START_TIME_MILLIS + 10 seconds)
  public static final long FIXED_CURRENT_TIME_MILLIS = START_TIME_MILLIS + 10_000L;

  private final Project project = new Project(1, "test-project");
  private String azkabanName = "unit-tests";
  private String scheme = "http";
  private String clientHostname = "localhost";
  private String clientPortNumber = "8081";

  private Flow flow;
  private ExecutableFlow executableFlow;
  private ExecutionOptions options;
  private EmailMessage message;
  private TimeZone defaultTz;

  private String read(final String file) throws Exception {
    final InputStream is = MailCreatorsTest.class.getResourceAsStream(file);
    return IOUtils.toString(is, Charsets.UTF_8).trim();
  }

  private void setTimeUTC() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    assertEquals("UTC", TimeZone.getDefault().getID());
    DateTimeUtils.setCurrentMillisFixed(FIXED_CURRENT_TIME_MILLIS);
    assertEquals(FIXED_CURRENT_TIME_MILLIS, DateTimeUtils.currentTimeMillis());
  }

  private void setUpDefault() throws Exception {
    this.defaultTz = TimeZone.getDefault();
    assertNotNull(this.defaultTz);

    setTimeUTC();

    this.flow = new Flow("mail-creator-test");
    this.options = new ExecutionOptions();
    this.message = new EmailMessage();

    final Node failedNode = new Node("test-job");
    failedNode.setType("noop");
    this.flow.addNode(failedNode);

    this.executableFlow = new ExecutableFlow(this.project, this.flow);
    this.executableFlow.setExecutionOptions(this.options);
    this.executableFlow.setStartTime(START_TIME_MILLIS);
    this.executableFlow.setSubmitTime(SUBMIT_TIME_MILLIS);

    this.options.setFailureEmails(ImmutableList.of("test@example.com"));
    this.options.setSuccessEmails(ImmutableList.of("test@example.com"));
  }

  private void setUpTemplate() throws Exception {
    this.defaultTz = TimeZone.getDefault();
    assertNotNull(this.defaultTz);

    setTimeUTC();

    MailCreatorRegistry.registerCreator(TemplateBasedMailCreator.fromResources("TemplateBasedMailCreator"), true);

    this.flow = new Flow("mail-creator-test");
    this.options = new ExecutionOptions();
    this.message = new EmailMessage();

    final Node failedNode = new Node("test-job");
    failedNode.setType("noop");
    this.flow.addNode(failedNode);

    this.executableFlow = new ExecutableFlow(this.project, this.flow);
    this.executableFlow.setExecutionOptions(this.options);
    this.executableFlow.setStartTime(START_TIME_MILLIS);
    this.executableFlow.setSubmitTime(SUBMIT_TIME_MILLIS);

    Map<String, String> flowParameters = new HashMap<String, String>();
    flowParameters.put("a", "b");
    flowParameters.put("c", "d");
    this.options.addAllFlowParameters(flowParameters);
    this.options.setFailureEmails(ImmutableList.of("test@example.com"));
    this.options.setSuccessEmails(ImmutableList.of("test@example.com"));
  }

  @After
  public void tearDown() throws Exception {
    if (this.defaultTz != null) {
      TimeZone.setDefault(this.defaultTz);
    }
    DateTimeUtils.setCurrentMillisSystem();
  }

  private void setJobStatus(final Status status) {
    this.executableFlow.getExecutableNodes().get(0).setStatus(status);
  }

  @Test
  public void defaultCreateErrorEmail() throws Exception {
    setUpDefault();
    setJobStatus(Status.FAILED);
    this.executableFlow.setEndTime(END_TIME_MILLIS);
    this.executableFlow.setStatus(Status.FAILED);
    assertTrue(new DefaultMailCreator().createErrorEmail(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has failed on unit-tests", this.message.getSubject());
    assertThat(read("default/errorEmail.html")).isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void defaultCreateFirstErrorMessage() throws Exception {
    setUpDefault();
    setJobStatus(Status.FAILED);
    this.executableFlow.setStatus(Status.FAILED_FINISHING);
    assertTrue(new DefaultMailCreator().createFirstErrorMessage(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has encountered a failure on unit-tests",
        this.message.getSubject());
    assertThat(read("default/firstErrorMessage.html"))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void defaultCreateSuccessEmail() throws Exception {
    setUpDefault();
    setJobStatus(Status.SUCCEEDED);
    this.executableFlow.setEndTime(END_TIME_MILLIS);
    this.executableFlow.setStatus(Status.SUCCEEDED);
    assertTrue(new DefaultMailCreator().createSuccessEmail(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has succeeded on unit-tests", this.message.getSubject());
    assertThat(read("default/successEmail.html"))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void templateBasedCreateErrorEmail() throws Exception {
    setUpTemplate();
    setJobStatus(Status.FAILED);
    this.executableFlow.setEndTime(END_TIME_MILLIS);
    this.executableFlow.setStatus(Status.FAILED);
    assertTrue(MailCreatorRegistry.getRecommendedCreator().createErrorEmail(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("test-project / mail-creator-test has failed on unit-tests",
        this.message.getSubject());
    assertThat(read("templateBased/errorEmail.html"))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void templateBasedCreateFirstErrorMessage() throws Exception {
    setUpTemplate();
    setJobStatus(Status.FAILED);
    this.executableFlow.setStatus(Status.FAILED_FINISHING);
    assertTrue(MailCreatorRegistry.getRecommendedCreator().createFirstErrorMessage(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("test-project / mail-creator-test has encountered a failure on unit-tests",
        this.message.getSubject());
    assertThat(read("templateBased/firstErrorMessage.html"))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void templateBasedCreateSuccessEmail() throws Exception {
    setUpTemplate();
    setJobStatus(Status.SUCCEEDED);
    this.executableFlow.setEndTime(END_TIME_MILLIS);
    this.executableFlow.setStatus(Status.SUCCEEDED);
    assertTrue(MailCreatorRegistry.getRecommendedCreator().createSuccessEmail(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("test-project / mail-creator-test has succeeded on unit-tests",
        this.message.getSubject());
    assertThat(read("templateBased/successEmail.html"))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

}
