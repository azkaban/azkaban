package azkaban;
import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.EventType;
import azkaban.utils.Props;
import org.junit.Test;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_CLASS_PARAM;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_EVENT_REPORTING_ENABLED;

public class AzkabanCommonModuleTest {
    /**
     * Verify that alternate implementation of the <code>AzkabanEventReporter</code>
     * is initialized.
     */
    @Test
    public void testCreateAzkabanEventReporter() {
        final Props props = new Props();
        props.put(AZKABAN_EVENT_REPORTING_ENABLED, "true");
        props.put(AZKABAN_EVENT_REPORTING_CLASS_PARAM,
                "azkaban.AzkabanEventReporterTest1");
        final AzkabanCommonModule azkabanCommonModule = new AzkabanCommonModule(props);
        final AzkabanEventReporter azkabanEventReporter = azkabanCommonModule
                .createAzkabanEventReporter();
        assertThat(azkabanEventReporter).isNotNull();
        assertThat(azkabanEventReporter).isInstanceOf(AzkabanEventReporterTest1.class);
    }
    /**
     * Verify that <code>IllegalArgumentException</code> is thrown when required properties
     * are missing.
     */
    @Test
    public void testAzkabanEventReporterInvalidProperties() {
        final Props props = new Props();
        props.put(AZKABAN_EVENT_REPORTING_ENABLED, "true");
        props.put(AZKABAN_EVENT_REPORTING_CLASS_PARAM,
                "azkaban.execapp.reporter.AzkabanKafkaAvroEventReporter");
        final AzkabanCommonModule azkabanCommonModule = new AzkabanCommonModule(props);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> azkabanCommonModule.createAzkabanEventReporter());
    }
    /**
     * Verify that a <code>RuntimeException</code> is thrown when valid constructor is
     * not found in the event reporter implementation.
     */
    @Test
    public void testAzkabanEventReporterInvalidConstructor() {
        final Props props = new Props();
        props.put(AZKABAN_EVENT_REPORTING_ENABLED, "true");
        props.put(AZKABAN_EVENT_REPORTING_CLASS_PARAM,
                "azkaban.execapp.AzkabanEventReporterTest3");
        AzkabanCommonModule azkabanCommonModule = new AzkabanCommonModule(props);
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> azkabanCommonModule.createAzkabanEventReporter());
    }
    /**
     * Ensures that the event reporter is not initialized when the property 'event.reporter.enabled'
     * is not set.
     */
    @Test
    public void testEventReporterDisabled() {
        final Props props = new Props();
        AzkabanCommonModule AzkabanCommonModule = new AzkabanCommonModule(props);
        final AzkabanEventReporter azkabanEventReporter = AzkabanCommonModule
                .createAzkabanEventReporter();
        assertThat(azkabanEventReporter).isNull();
    }
}
// Dummy implementation of the AzkabanEventReporter interface
// with valid constructor.
class AzkabanEventReporterTest1 implements AzkabanEventReporter {
    public AzkabanEventReporterTest1(final Props props) {
    }
    @Override
    public boolean report(final EventType eventType, final Map<String, String> metadata) {
        return false;
    }
}
// Dummy implementation of the AzkabanEventReporter interface, for test.
// Valid constructor is not available.
class AzkabanEventReporterTest2 implements AzkabanEventReporter {
    @Override
    public boolean report(final EventType eventType, final Map<String, String> metadata) {
        return false;
    }
}
// Dummy implementation of the AzkabanEventReporter with an invalid constructor
class AzkabanEventReporterTest3 implements AzkabanEventReporter {
    public AzkabanEventReporterTest3() {
    }
    @Override
    public boolean report(final EventType eventType, final Map<String, String> metadata) {
        return false;
    }
}
