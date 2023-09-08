package azkaban.scheduler;

import azkaban.Constants;
import azkaban.executor.ExecutionOptions;
import azkaban.metrics.MetricsManager;
import azkaban.sla.SlaAction;
import azkaban.sla.SlaOption;
import azkaban.sla.SlaType;
import azkaban.utils.Emailer;
import azkaban.utils.Props;
import com.codahale.metrics.Meter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public class ScheduleChangeEmailerManagerTest {
  ScheduleChangeEmailerManager scheduleChangeEmailerManager;
  MetricsManager metricsManager;
  Meter fakeMeter;
  Emailer emailer;

  @Before
  public void setup() {
    this.emailer = mock(Emailer.class);
    this.metricsManager = mock(MetricsManager.class);
    this.fakeMeter = new Meter();
    final Props azkProps = new Props();
    azkProps.put(Constants.SCHEDULE_EMAILER_ENABLED, "true");
    when(this.metricsManager.addMeter(any())).thenReturn(this.fakeMeter);

    this.scheduleChangeEmailerManager = new ScheduleChangeEmailerManager(azkProps, this.emailer, this.metricsManager);
    this.scheduleChangeEmailerManager.start();
  }

  @After
  public void cleanup() throws InterruptedException {
    this.scheduleChangeEmailerManager.stop();
  }

  @Test
  public void testGetEmailRecipientsFromSchedule() {
    String flowName = "testFlow";
    String email = "test-azkaban-sla@linkedin.com";
    long firstSchedTime = System.currentTimeMillis();
    long endSchedTime = firstSchedTime + 100000;
    ExecutionOptions executionOptions = new ExecutionOptions();
    final List<SlaOption> slaOptions = new ArrayList<>();
    slaOptions.add(
        new SlaOption.SlaOptionBuilder(SlaType.FLOW_FINISH, flowName, Duration.ofHours(1))
            .setActions(Collections.singleton(SlaAction.KILL))
            .setEmails(Collections.singletonList(email))
            .createSlaOption());
    executionOptions.setSlaOptions(slaOptions);
    Schedule schedule = new Schedule(1, 123, "testProject", flowName,"ready",
        firstSchedTime, endSchedTime, DateTimeZone.getDefault(), null, DateTime.now().getMillis(), firstSchedTime,
        firstSchedTime, "testUser", executionOptions, "0 * * * * *", false);
    assertThat(this.scheduleChangeEmailerManager.getEmailRecipientsFromSchedule(schedule)).contains(email);
  }

  @Test
  public void testGetEmailRecipientsWithoutSLAEmail() {
    String flowName = "testFlow";
    String email = "test-azkaban@linkedin.com";
    long firstSchedTime = System.currentTimeMillis();
    long endSchedTime = firstSchedTime + 100000;
    ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setFailureEmails(Collections.singletonList(email));
    Schedule schedule = new Schedule(1, 123, "testProject", flowName,"ready",
        firstSchedTime, endSchedTime, DateTimeZone.getDefault(), null, DateTime.now().getMillis(), firstSchedTime,
        firstSchedTime, "testUser", executionOptions, "0 * * * * *", false);
    assertThat(this.scheduleChangeEmailerManager.getEmailRecipientsFromSchedule(schedule)).contains(email);
  }
  @Test
  public void testAddEmailTask() {
    String flowName = "testFlow";
    String email = "test-azkaban@linkedin.com";
    long firstSchedTime = System.currentTimeMillis();
    long endSchedTime = firstSchedTime + 100000;
    ExecutionOptions executionOptions = new ExecutionOptions();
    executionOptions.setFailureEmails(Collections.singletonList(email));
    Schedule schedule = new Schedule(1, 123, "testProject", flowName,"ready",
        firstSchedTime, endSchedTime, DateTimeZone.getDefault(), null, DateTime.now().getMillis(), firstSchedTime,
        firstSchedTime, "testUser", executionOptions, "0 * * * * *", false);
    // verify task is added into queue
    assertThatCode(
        () -> scheduleChangeEmailerManager.addNotificationTaskOnScheduleDelete(schedule, "test", "Project Removal")
    ).doesNotThrowAnyException();
  }
}
