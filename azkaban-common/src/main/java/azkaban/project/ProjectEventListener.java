package azkaban.project;
import azkaban.ServiceProvider;
import azkaban.event.EventListener;
import azkaban.spi.AzkabanEventReporter;
import azkaban.utils.Props;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;

import static azkaban.Constants.AZ_WEBSERVER;
import static azkaban.Constants.ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME;

public class ProjectEventListener implements EventListener<ProjectEvent> {

    private Map<String, String> getProjectMetadata(final ProjectEvent event){
        final Map<String, String> metaData = new HashMap<>();
        final Map<String, Object> projectMetaData = event.getEventData();
        final Props props = ServiceProvider.SERVICE_PROVIDER.getInstance(Props.class);
        // Set up properties not in eventData
        metaData.put(AZ_WEBSERVER, props.getString(AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME,
                props.getString("jetty.hostname", "localhost")));
        metaData.put("projectLogEventType", event.getType().toString());

        // Fill up metaData with event specific data
        for(String key: projectMetaData.keySet()) {
            metaData.put(key, String.valueOf(projectMetaData.get(key)));
        }

        return metaData;
    }

    @Override
    public void handleEvent(ProjectEvent event){
        final Project project = event.getProject();
        AzkabanEventReporter azkabanEventReporter = project.getAzkabanEventReporter();
        // We can handle different event types in the getProjectMetadata function
        azkabanEventReporter.report(event.getType(), getProjectMetadata(event));
    }
}
