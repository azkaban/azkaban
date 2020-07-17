package azkaban.project;

import azkaban.spi.AzkabanEventReporter;
import azkaban.spi.EventType;
import azkaban.utils.Props;

import java.util.Map;

// Dummy implementation of the AzkabanEventReporter interface
// with valid constructor.
public class AzkabanEventReporterTest implements AzkabanEventReporter {
    public AzkabanEventReporterTest(final Props props) {
    }
    @Override
    public boolean report(final EventType eventType, final Map<String, String> metadata) {
        return false;
    }
}
