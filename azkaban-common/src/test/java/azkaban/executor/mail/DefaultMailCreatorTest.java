package azkaban.executor.mail;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.Status;
import azkaban.flow.Flow;
import azkaban.flow.Node;
import azkaban.project.Project;
import azkaban.utils.EmailMessage;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

  @Before
  public void setUp() throws Exception {
    defaultTz = TimeZone.getDefault();
    assertNotNull(defaultTz);
    // EEST
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Helsinki"));
    DateTimeUtils.setCurrentMillisFixed(FIXED_CURRENT_TIME_MILLIS);

    mailCreator = new DefaultMailCreator();

    flow = new Flow("mail-creator-test");
    project = new Project(1, "test-project");
    options = new ExecutionOptions();
    message = new EmailMessage();

    azkabanName = "unit-tests";
    scheme = "http";
    clientHostname = "localhost";
    clientPortNumber = "8081";

    Node failedNode = new Node("test-job");
    failedNode.setType("noop");
    flow.addNode(failedNode);

    executableFlow = new ExecutableFlow(project, flow);
    executableFlow.setExecutionOptions(options);
    executableFlow.setStartTime(START_TIME_MILLIS);

    options.setFailureEmails(ImmutableList.of("test@example.com"));
    options.setSuccessEmails(ImmutableList.of("test@example.com"));
  }

  @After
  public void tearDown() throws Exception {
    if (defaultTz != null) {
      TimeZone.setDefault(defaultTz);
    }
    DateTimeUtils.setCurrentMillisSystem();
  }

  private void setJobStatus(Status status) {
    executableFlow.getExecutableNodes().get(0).setStatus(status);
  }

  @Test
  public void createErrorEmail() throws Exception {
    setJobStatus(Status.FAILED);
    executableFlow.setEndTime(END_TIME_MILLIS);
    executableFlow.setStatus(Status.FAILED);
    assertTrue(mailCreator.createErrorEmail(
        executableFlow, message, azkabanName, scheme, clientHostname, clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has failed on unit-tests", message.getSubject());
    assertEquals(read("errorEmail.html"), message.getBody());
  }

  @Test
  public void createFirstErrorMessage() throws Exception {
    setJobStatus(Status.FAILED);
    executableFlow.setStatus(Status.FAILED_FINISHING);
    assertTrue(mailCreator.createFirstErrorMessage(
        executableFlow, message, azkabanName, scheme, clientHostname, clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has encountered a failure on unit-tests", message.getSubject());
    assertEquals(read("firstErrorMessage.html"), message.getBody());
  }

  @Test
  public void createSuccessEmail() throws Exception {
    setJobStatus(Status.SUCCEEDED);
    executableFlow.setEndTime(END_TIME_MILLIS);
    executableFlow.setStatus(Status.SUCCEEDED);
    assertTrue(mailCreator.createSuccessEmail(
        executableFlow, message, azkabanName, scheme, clientHostname, clientPortNumber));
    assertEquals("Flow 'mail-creator-test' has succeeded on unit-tests", message.getSubject());
    assertEquals(read("successEmail.html"), message.getBody());
  }

  private String read(String file) throws Exception {
    InputStream is = DefaultMailCreatorTest.class.getResourceAsStream(file);
    return IOUtils.toString(is, Charsets.UTF_8).trim();
  }

}
