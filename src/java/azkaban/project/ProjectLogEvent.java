package azkaban.project;

public class ProjectLogEvent {
	/**
	 * Log event type messages. Do not change the numeric representation of each enum.
	 * 
	 * Only represent from 0 to 255 different codes.
	 */
	public static enum EventType {
		ERROR(128), CREATED(1), DELETED(2), USER_PERMISSION(3), GROUP_PERMISSION(4), DESCRIPTION(5), UPLOADED(6), SCHEDULE(7), SLA(8);

		private int numVal;

		EventType(int numVal) {
			this.numVal = numVal;
		}

		public int getNumVal() {
			return numVal;
		}

		public static EventType fromInteger(int x) {
			switch (x) {
			case 1:
				return CREATED;
			case 2:
				return DELETED;
			case 3:
				return USER_PERMISSION;
			case 4:
				return GROUP_PERMISSION;
			case 5:
				return DESCRIPTION;
			case 6:
				return UPLOADED;
			case 7:
				return SCHEDULE;
			case 8:
				return SLA;
			case 128:
				return ERROR;
			default:
				return ERROR;
			}
		}
	}
	
	private final int projectId;
	private final String user;
	private final long time;
	private final EventType type;
	private final String message;

	public ProjectLogEvent(int projectId, EventType type, long time, String user, String message) {
		this.projectId = projectId;
		this.user = user;
		this.time = time;
		this.type = type;
		this.message = message;
	}

	public int getProjectId() {
		return projectId;
	}

	public String getUser() {
		return user;
	}

	public long getTime() {
		return time;
	}

	public EventType getType() {
		return type;
	}

	public String getMessage() {
		return message;
	}

}