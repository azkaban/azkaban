/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

public class JSONUtils {

  /**
   * The constructor. Cannot construct this class.
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

  public static void toJSON(Object obj, File file, boolean prettyPrint)
      throws IOException {
    BufferedOutputStream stream =
        new BufferedOutputStream(new FileOutputStream(file));
    try {
      toJSON(obj, stream, prettyPrint);
    } finally {
      stream.close();
    }
  }

  public static Object parseJSONFromStringQuiet(String json) {
    try {
      return parseJSONFromString(json);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
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

  public static Object parseJSONFromReader(Reader reader) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = new JsonFactory();
    JsonParser parser = factory.createJsonParser(reader);
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
        System.err.println("ERROR What is this!? " + node.getNumberType());
        return null;
      }
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else {
      return null;
    }
  }

  public static long getLongFromObject(Object obj) {
    if (obj instanceof Integer) {
      return Long.valueOf((Integer) obj);
    }

    return (Long) obj;
  }

  /*
   * Writes json to a stream without using any external dependencies.
   *
   * This is useful for plugins or extensions that want to write properties to a
   * writer without having to import the jackson, or json libraries. The
   * properties are expected to be a map of String keys and String values.
   *
   * The other json writing methods are more robust and will handle more cases.
   */
  public static void writePropsNoJarDependency(Map<String, String> properties,
      Writer writer) throws IOException {
    writer.write("{\n");
    int size = properties.size();

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      // tab the space
      writer.write('\t');
      // Write key
      writer.write(quoteAndClean(entry.getKey()));
      writer.write(':');
      writer.write(quoteAndClean(entry.getValue()));

      size -= 1;
      // Add comma only if it's not the last one
      if (size > 0) {
        writer.write(',');
      }
      writer.write('\n');
    }
    writer.write("}");
  }

  private static String quoteAndClean(String str) {
    if (str == null || str.isEmpty()) {
      return "\"\"";
    }

    StringBuffer buffer = new StringBuffer(str.length());
    buffer.append('"');
    for (int i = 0; i < str.length(); ++i) {
      char ch = str.charAt(i);

      switch (ch) {
      case '\b':
        buffer.append("\\b");
        break;
      case '\t':
        buffer.append("\\t");
        break;
      case '\n':
        buffer.append("\\n");
        break;
      case '\f':
        buffer.append("\\f");
        break;
      case '\r':
        buffer.append("\\r");
        break;
      case '"':
      case '\\':
      case '/':
        buffer.append('\\');
        buffer.append(ch);
        break;
      default:
        if (isCharSpecialUnicode(ch)) {
          buffer.append("\\u");
          String hexCode = Integer.toHexString(ch);
          int lengthHexCode = hexCode.length();
          if (lengthHexCode < 4) {
            buffer.append("0000".substring(0, 4 - lengthHexCode));
          }
          buffer.append(hexCode);
        } else {
          buffer.append(ch);
        }
      }
    }
    buffer.append('"');
    return buffer.toString();
  }

  private static boolean isCharSpecialUnicode(char ch) {
    if (ch < ' ') {
      return true;
    } else if (ch >= '\u0080' && ch < '\u00a0') {
      return true;
    } else if (ch >= '\u2000' && ch < '\u2100') {
      return true;
    }

    return false;
  }
}
