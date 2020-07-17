package azkaban.project;
import azkaban.spi.EventType;
import com.google.common.base.Preconditions;

import java.util.Map;

public class ProjectEvent{
    private final Project project;
    private final EventType eventType;
    private final long time;

    private final Map<String, Object> eventData;

    private ProjectEvent(Project project, final EventType eventType, final Map<String, Object> eventData) {
        this.project = project;
        this.eventType = eventType;
        this.time = System.currentTimeMillis();
        this.eventData = eventData;
    }
    /**
     * Creates a new event.
     *
     * @param project
     * @param eventType
     * @param eventData
     * @return New ProjectEvent instance.
     * @throws NullPointerException if EventData is null.
     */
    public static ProjectEvent create(final Project project, final EventType eventType, final Map<String, Object> eventData)
            throws NullPointerException {
        Preconditions.checkNotNull(eventData, "EventData was null");
        return new ProjectEvent(project, eventType, eventData);
    }

    public Project getProject(){ return this.project; }

    public EventType getType() {
        return this.eventType;
    }

    public long getTime() { return this.time; }

    public Map<String, Object> getEventData() { return eventData; }
}
