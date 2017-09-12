package azkaban.execapp.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import azkaban.event.Event;
import azkaban.event.EventData;
import azkaban.execapp.EventReporterUtil;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.EventType;
import java.util.HashMap;
import org.junit.Test;

/**
 * Tests for the default implementation of <code>AzkabanEventReporter</code> class.
 */
public class AzkabanEventReporterTest {


  @Test
  public void testAzkabanKafkaEventReporter() {
    final AzkabanEventReporter azkabanKafkaAvroEventReporter
        = EventReporterUtil.getTestAzkabanEventReporter();
    final EventData eventData = mock(EventData.class);

    final Object runnerObject = new Object();
    boolean status;

    // test flow started event
    Event event = Event.create(runnerObject, EventType.FLOW_STARTED, eventData);
    status = azkabanKafkaAvroEventReporter.report(event.getType(), new HashMap<>());
    assertThat(status).isTrue();

    // test flow finished event
    event = Event.create(runnerObject, EventType.FLOW_FINISHED, eventData);
    status = azkabanKafkaAvroEventReporter.report(event.getType(), new HashMap<>());
    assertThat(status).isTrue();

    // test job started event
    event = Event.create(runnerObject, EventType.JOB_STARTED, eventData);
    status = azkabanKafkaAvroEventReporter.report(event.getType(), new HashMap<>());
    assertThat(status).isTrue();

    // test job finished event
    event = Event.create(runnerObject, EventType.JOB_FINISHED, eventData);
    status = azkabanKafkaAvroEventReporter.report(event.getType(), new HashMap<>());
    assertThat(status).isTrue();

    // test un-supported event type
    event = Event.create(runnerObject, EventType.JOB_STATUS_CHANGED, eventData);
    status = azkabanKafkaAvroEventReporter.report(event.getType(), new HashMap<>());
    assertThat(status).isFalse();
  }

  @Test
  public void testNullAzkabanKafkaEventReporter() {
    final AzkabanKafkaAvroEventReporter azkabanKafkaAvroEventReporter
        = new AzkabanKafkaAvroEventReporter(null, EventReporterUtil.getTestKafkaProps());
    final Event event = mock(Event.class);
    final boolean status = azkabanKafkaAvroEventReporter.report(event.getType(), new HashMap<>());
    assertThat(status).isFalse();
  }

}
