package azkaban.utils;

import org.codehaus.jackson.map.ObjectMapper;

public class JSONUtils {
	/**
	 * Prevent the instantiation of this helper class.
	 */
	private JSONUtils() {
	}
	
	public static String toJSON(Object obj) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(obj);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
