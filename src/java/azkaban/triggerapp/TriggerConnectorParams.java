package azkaban.triggerapp;

public interface TriggerConnectorParams {
	public static final String ACTION_PARAM = "action";
	public static final String TRIGGER_ID_PARAM = "triggerid";
	public static final String USER_PARAM = "user";
	
	public static final String PING_ACTION = "ping";
	
	public static final String INSERT_TRIGGER_ACTION = "insert";
	public static final String REMOVE_TRIGGER_ACTION = "remove";
	public static final String UPDATE_TRIGGER_ACTION = "update";
	public static final String GET_UPDATE_ACTION = "getupdate";
	
	public static final String RESPONSE_NOTFOUND = "notfound";
	public static final String RESPONSE_ERROR = "error";
	public static final String RESPONSE_SUCCESS = "success";
	public static final String RESPONSE_ALIVE = "alive";
	public static final String RESPONSE_UPDATETIME = "lasttime";
	public static final String RESPONSE_UPDATED_TRIGGERS = "updated";
	
	public static final String UPDATE_TIME_LIST_PARAM = "updatetime";
	
	public static final String JMX_GET_MBEANS = "getMBeans";
	public static final String JMX_GET_MBEAN_INFO = "getMBeanInfo";
	public static final String JMX_GET_MBEAN_ATTRIBUTE = "getAttribute";
	public static final String JMX_GET_ALL_MBEAN_ATTRIBUTES = "getAllMBeanAttributes";
	public static final String JMX_ATTRIBUTE = "attribute";
	public static final String JMX_MBEAN = "mBean";
	
	public static final String JMX_GET_ALL_TRIGGER_SERVER_ATTRIBUTES = "getAllTriggerServerAttributes";
	public static final String JMX_HOSTPORT = "hostPort";
}
