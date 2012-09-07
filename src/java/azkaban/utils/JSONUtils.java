package azkaban.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JSONUtils {

	
	/**
	 * The constructor. Cannot construct this class.
	 */
	private JSONUtils() {
	}
	
	/**
	 * Takes a reader to stream the JSON string. The reader is not wrapped in a BufferReader
	 * so it is up to the user to employ such optimizations if so desired.
	 * 
	 * The results will be Maps, Lists and other Java mapping of Json types (i.e. String, Number, Boolean).
	 * 
	 * @param reader
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> fromJSONStream(Reader reader) throws Exception {
		JSONObject jsonObj = new JSONObject(new JSONTokener(reader));
		Map<String, Object> results = createObject(jsonObj);
		
		return results;
	}
	
	/**
	 * Converts a json string to Objects.
	 * 
	 * The results will be Maps, Lists and other Java mapping of Json types (i.e. String, Number, Boolean).
	 * 
	 * @param str
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> fromJSONString(String str) throws Exception {
		JSONObject jsonObj = new JSONObject(str);
		Map<String, Object> results = createObject(jsonObj);
		return results;
	}

	/**
	 * Recurses through the json object to create a Map/List/Object equivalent.
	 * 
	 * @param obj
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> createObject(JSONObject obj) {
		LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
		
		Iterator<String> iterator = obj.keys();
		while(iterator.hasNext()) {
			String key = iterator.next();
			Object value = null;
			try {
				value = obj.get(key);
			} catch (JSONException e) {
				// Since we get the value from the key given by the JSONObject, 
				// this exception shouldn't be thrown.
			}
			
			if (value instanceof JSONArray) {
				value = createArray((JSONArray)value);
			}
			else if (value instanceof JSONObject) {
				value = createObject((JSONObject)value);
			}
			
			map.put(key, value);
		}

		return map;
	}
	
	/**
	 * Recurses through the json object to create a Map/List/Object equivalent.
	 * 
	 * @param obj
	 * @return
	 */
	private static List<Object> createArray(JSONArray array) {
		ArrayList<Object> list = new ArrayList<Object>();
		for (int i = 0; i < array.length(); ++i) {
			Object value = null;
			try {
				value = array.get(i);
			} catch (JSONException e) {
				// Ugh... JSON's over done exception throwing.
			}
			
			if (value instanceof JSONArray) {
				value = createArray((JSONArray)value);
			}
			else if (value instanceof JSONObject) {
				value = createObject((JSONObject)value);
			}
			
			list.add(value);
		}
		
		return list;
	}
	
	/**
	 * Creates a json string from Map/List/Primitive object.
	 * 
	 * @param obj
	 * @return
	 */
	public static String toJSONString(List<?> list) {
		JSONArray jsonList = new JSONArray(list);
		try {
			return jsonList.toString();
		} catch (Exception e) {
			return "";
		}
	}
	
	/**
	 * Creates a json string from Map/List/Primitive object.
	 * 
	 * @param obj
	 * @parm indent
	 * @return
	 */
	public static String toJSONString(List<?> list, int indent) {
		JSONArray jsonList = new JSONArray(list);
		try {
			return jsonList.toString(indent);
		} catch (Exception e) {
			return "";
		}
	}
	
	/**
	 * Creates a json string from Map/List/Primitive object.
	 * 
	 * @param obj
	 * @return
	 */
	public static String toJSONString(Map<String, Object> obj) {
		JSONObject jsonObj = new JSONObject(obj);
		try {
			return jsonObj.toString();
		} catch (Exception e) {
			return "";
		}
	}
	
	/**
	 * Creates a json pretty string from Map/List/Primitive object
	 * 
	 * @param obj
	 * @param indent
	 * @return
	 */
	public static String toJSONString(Map<String, Object> obj, int indent) {
		JSONObject jsonObj = new JSONObject(obj);
		try {
			return jsonObj.toString(indent);
		} catch (Exception e) {
			return "";
		}
	}
	
	public static String toJSON(Object obj) {
		return toJSON(obj, false);
	}

	public static String toJSON(Object obj, boolean prettyPrint) {
		ObjectMapper mapper = new ObjectMapper();

		try {
			if (prettyPrint) {
				ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
				return writer.writeValueAsString(obj);
			}
			return mapper.writeValueAsString(obj);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void toJSON(Object obj, OutputStream stream) {
		toJSON(obj, stream, false);
	}
	
	public static void toJSON(Object obj, OutputStream stream, boolean prettyPrint) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			if (prettyPrint) {
				ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
				writer.writeValue(stream, obj);
				return;
			}
			mapper.writeValue(stream, obj);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void toJSON(Object obj, File file) throws IOException {
		toJSON(obj, file, false);
	}
	
	public static void toJSON(Object obj, File file, boolean prettyPrint) throws IOException {
		BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(file));
		toJSON(obj, stream, prettyPrint);
		stream.close();
	}
	
	public static Object parseJSONFromString(String json) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = new JsonFactory();
		JsonParser parser = factory.createJsonParser(json);
		JsonNode node = mapper.readTree(parser);

		return toObjectFromJSONNode(node);
	}

	public static Object parseJSONFromFile(File file) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory factory = new JsonFactory();
		JsonParser parser = factory.createJsonParser(file);
		JsonNode node = mapper.readTree(parser);

		return toObjectFromJSONNode(node);
	}

	private static Object toObjectFromJSONNode(JsonNode node) {
		if (node.isObject()) {
			HashMap<String, Object> obj = new HashMap<String, Object>();
			Iterator<String> iter = node.getFieldNames();
			while (iter.hasNext()) {
				String fieldName = iter.next();
				JsonNode subNode = node.get(fieldName);
				Object subObj = toObjectFromJSONNode(subNode);
				obj.put(fieldName, subObj);
			}

			return obj;
		} else if (node.isArray()) {
			ArrayList<Object> array = new ArrayList<Object>();
			Iterator<JsonNode> iter = node.getElements();
			while (iter.hasNext()) {
				JsonNode element = iter.next();
				Object subObject = toObjectFromJSONNode(element);
				array.add(subObject);
			}
			return array;
		} else if (node.isTextual()) {
			return node.asText();
		} else if (node.isNumber()) {
			if (node.isInt()) {
				return node.asInt();
			} else if (node.isLong()) {
				return node.asLong();
			} else if (node.isDouble()) {
				return node.asDouble();
			} else {
				System.err.println("ERROR What is this!? "
						+ node.getNumberType());
				return null;
			}
		} else if (node.isBoolean()) {
			return node.asBoolean();
		} else {
			return null;
		}
	}
}
