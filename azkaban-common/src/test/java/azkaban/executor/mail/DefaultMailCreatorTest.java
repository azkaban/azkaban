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
import azkaban.utils.EmailMessageCreator;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import java.io.InputStream;
import java.util.TimeZone;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DefaultMailCreatorTest {

  // 2016/07/17 11:54:11 EEST
  public static final long START_TIME_MILLIS = 1468745651608L;
  // 2016/07/17 11:54:16 EEST (START_TIME_MILLIS + 5 seconds)
  public static final long END_TIME_MILLIS = START_TIME_MILLIS + 5_000L;
  // 2016/07/17 11:54:21 EEST (START_TIME_MILLIS + 10 seconds)
  public static final long FIXED_CURRENT_TIME_MILLIS = START_TIME_MILLIS + 10_000L;

  private DefaultMailCreator mailCreator;

  private ExecutableFlow executableFlow;
  private Flow flow;
  private Project project;
  private ExecutionOptions options;
  private EmailMessage message;
  private String azkabanName;
  private String scheme;
  private String clientHostname;
  private String clientPortNumber;
  private TimeZone defaultTz;

  public static String read(final String file) throws Exception {
    final InputStream is = DefaultMailCreatorTest.class.getResourceAsStream(file);
    return IOUtils.toString(is, Charsets.UTF_8).trim();
  }

  @Before
  public void setUp() throws Exception {
    this.defaultTz = TimeZone.getDefault();
    assertNotNull(this.defaultTz);
    // EEST
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
    DateTimeUtils.setCurrentMillisFixed(FIXED_CURRENT_TIME_MILLIS);

    this.mailCreator = new DefaultMailCreator();

    this.flow = new Flow("mail-creator-test");
    this.project = new Project(1, "test-project");
    this.options = new ExecutionOptions();
    this.message = new EmailMessage("localhost", EmailMessageCreator.DEFAULT_SMTP_PORT, "", "",
        null);

    this.azkabanName = "unit-tests";
    this.scheme = "http";
    this.clientHostname = "localhost";
    this.clientPortNumber = "8081";

    final Node failedNode = new Node("test-job");
    failedNode.setType("noop");
    this.flow.addNode(failedNode);

    this.executableFlow = new ExecutableFlow(this.project, this.flow);
    this.executableFlow.setExecutionOptions(this.options);
    this.executableFlow.setStartTime(START_TIME_MILLIS);

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
  public void createErrorEmail() throws Exception {
    setJobStatus(Status.FAILED);
    this.executableFlow.setEndTime(END_TIME_MILLIS);
    this.executableFlow.setStatus(Status.FAILED);
    assertTrue(this.mailCreator.createErrorEmail(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has failed on unit-tests", this.message.getSubject());
    assertThat(read("errorEmail.html")).isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void createFirstErrorMessage() throws Exception {
    setJobStatus(Status.FAILED);
    this.executableFlow.setStatus(Status.FAILED_FINISHING);
    assertTrue(this.mailCreator.createFirstErrorMessage(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has encountered a failure on unit-tests",
        this.message.getSubject());
    assertThat(read("firstErrorMessage.html")).isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void createSuccessEmail() throws Exception {
    setJobStatus(Status.SUCCEEDED);
    this.executableFlow.setEndTime(END_TIME_MILLIS);
    this.executableFlow.setStatus(Status.SUCCEEDED);
    assertTrue(this.mailCreator.createSuccessEmail(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has succeeded on unit-tests", this.message.getSubject());
    assertThat(read("successEmail.html")).isEqualToIgnoringWhitespace(this.message.getBody());
  }

}
