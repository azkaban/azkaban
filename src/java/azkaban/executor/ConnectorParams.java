package azkaban.executor;

public interface ConnectorParams {
	public static final String ACTION_PARAM = "action";
	public static final String EXECID_PARAM = "execid";
	public static final String SHAREDTOKEN_PARAM = "token";
	public static final String USER_PARAM = "user";
	
	public static final String STATUS_ACTION = "status";
	public static final String EXECUTE_ACTION = "execute";
	public static final String CANCEL_ACTION = "cancel";
	public static final String PAUSE_ACTION = "pause";
	public static final String RESUME_ACTION = "resume";
	public static final String PING_ACTION = "ping";
	
	public static final String START_PARAM = "start";
	public static final String END_PARAM = "end";
	public static final String STATUS_PARAM = "status";
	public static final String NODES_PARAM = "nodes";
	public static final String EXECPATH_PARAM = "execpath";
	
	public static final String RESPONSE_NOTFOUND = "notfound";
	public static final String RESPONSE_ERROR = "error";
	public static final String RESPONSE_SUCCESS = "success";
	public static final String RESPONSE_ALIVE = "alive";
	public static final String RESPONSE_UPDATETIME = "lasttime";
	
	public static final int NODE_NAME_INDEX = 0;
	public static final int NODE_STATUS_INDEX = 1;
	public static final int NODE_START_INDEX = 2;
	public static final int NODE_END_INDEX = 3;

	public static final String FORCED_FAILED_MARKER = ".failed";
}
