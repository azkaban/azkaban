package azkaban.executor.mail;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.utils.EmailMessage;
import azkaban.utils.EmailMessageCreator;
import azkaban.utils.TestUtils;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
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

  private Executor executor;
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

  public static ExecutorManagerException createTestStracktrace() {
    final ExecutorManagerException exception = new ExecutorManagerException("mocked failure");
    // set custom stacktrace to have deterministic string for comparison
    exception.setStackTrace(new StackTraceElement[]{new StackTraceElement(
        DefaultMailCreatorTest.class.getCanonicalName(), "createFailedUpdateMessage",
        "DefaultMailCreatorTest.java", 135)});
    return exception;
  }

  @Before
  public void setUp() throws Exception {
    this.defaultTz = TimeZone.getDefault();
    assertNotNull(this.defaultTz);
    // EEST
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));

    this.mailCreator = new DefaultMailCreator();

    this.executor = new Executor(1, "executor1-host", 1234, true);
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
    final List<ExecutableFlow> executableFlows = new ArrayList<>();

    final ExecutableFlow executableFlow1 = new ExecutableFlow(this.project, this.flow);
    executableFlow1.setExecutionId(1);
    executableFlow1.setStartTime(START_TIME_MILLIS);
    executableFlow1.setEndTime(END_TIME_MILLIS);
    executableFlow1.setStatus(Status.FAILED);
    executableFlows.add(executableFlow1);

    final ExecutableFlow executableFlow2 = new ExecutableFlow(this.project, this.flow);
    executableFlow2.setExecutionId(2);
    executableFlow2.setStartTime(START_TIME_MILLIS);
    executableFlow2.setEndTime(END_TIME_MILLIS);
    executableFlow2.setStatus(Status.SUCCEEDED);
    executableFlows.add(executableFlow2);

    assertTrue(this.mailCreator.createErrorEmail(
        this.executableFlow, executableFlows, this.message, this.azkabanName, this.scheme, this
            .clientHostname, this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has FAILED on unit-tests", this.message.getSubject());
    assertThat(TestUtils.readResource("errorEmail.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void createKilledEmail() throws Exception {
    setJobStatus(Status.KILLED);
    this.executableFlow.setEndTime(END_TIME_MILLIS);
    this.executableFlow.setStatus(Status.KILLED);
    final List<ExecutableFlow> executableFlows = new ArrayList<>();

    final ExecutableFlow executableFlow1 = new ExecutableFlow(this.project, this.flow);
    executableFlow1.setExecutionId(1);
    executableFlow1.setStartTime(START_TIME_MILLIS);
    executableFlow1.setEndTime(END_TIME_MILLIS);
    executableFlow1.setStatus(Status.FAILED);
    executableFlows.add(executableFlow1);

    final ExecutableFlow executableFlow2 = new ExecutableFlow(this.project, this.flow);
    executableFlow2.setExecutionId(2);
    executableFlow2.setStartTime(START_TIME_MILLIS);
    executableFlow2.setEndTime(END_TIME_MILLIS);
    executableFlow2.setStatus(Status.SUCCEEDED);
    executableFlows.add(executableFlow2);

    assertTrue(this.mailCreator.createErrorEmail(
        this.executableFlow, executableFlows, this.message, this.azkabanName, this.scheme,
        this.clientHostname, this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' was KILLED on unit-tests", this.message.getSubject());
    assertThat(TestUtils.readResource("killedEmail.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void createFirstErrorMessage() throws Exception {
    setJobStatus(Status.FAILED);
    // TODO ypadron: implement ability to mock current time in java.time api and remove setEndTime()
    this.executableFlow.setEndTime(END_TIME_MILLIS);
    this.executableFlow.setStatus(Status.FAILED_FINISHING);
    assertTrue(this.mailCreator.createFirstErrorMessage(
        this.executableFlow, this.message, this.azkabanName, this.scheme, this.clientHostname,
        this.clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has encountered a failure on unit-tests",
        this.message.getSubject());
    assertThat(TestUtils.readResource("firstErrorMessage.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
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
    assertThat(TestUtils.readResource("successEmail.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void createFailedUpdateMessage() throws Exception {
    final ExecutorManagerException exception = createTestStracktrace();
    assertTrue(this.mailCreator
        .createFailedUpdateMessage(Arrays.asList(this.executableFlow, this.executableFlow),
            this.executor, exception, this.message, this.azkabanName, this.scheme,
            this.clientHostname, this.clientPortNumber));
    assertEquals("Flow status could not be updated from executor1-host on unit-tests",
        this.message.getSubject());
    assertThat(TestUtils.readResource("failedUpdateMessage.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

  @Test
  public void createFailedHealthCheckMessage() throws Exception {
    final ExecutorManagerException exception = createTestStracktrace();
    assertTrue(this.mailCreator
        .createFailedExecutorHealthCheckMessage(Arrays.asList(this.executableFlow, this.executableFlow),
            this.executor, exception, this.message, this.azkabanName, this.scheme,
            this.clientHostname, this.clientPortNumber, ImmutableList.of("test@example.com")));
    assertEquals("Alert: Executor is unreachable, executor1-host on unit-tests",
        this.message.getSubject());
    assertThat(TestUtils.readResource("failedExecutorHealthCheckMessage.html", this))
        .isEqualToIgnoringWhitespace(this.message.getBody());
  }

}
