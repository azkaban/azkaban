package azkaban.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

public class JSONUtils {
	/**
	 * Prevent the instantiation of this helper class.
	 */
	private JSONUtils() {
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
