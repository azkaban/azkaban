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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONUtils {

  private static final Logger log = LoggerFactory.getLogger(JSONUtils.class);

  /**
   * The constructor. Cannot construct this class.
   */
  private JSONUtils() {
  }

  public static String toJSON(final Object obj) {
    return toJSON(obj, false);
  }

  public static String toJSON(final Object obj, final boolean prettyPrint) {
    final ObjectMapper mapper = new ObjectMapper();

    try {
      if (prettyPrint) {
        final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        return writer.writeValueAsString(obj);
      }
      return mapper.writeValueAsString(obj);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void toJSON(final Object obj, final OutputStream stream) {
    toJSON(obj, stream, false);
  }

  public static void toJSON(final Object obj, final OutputStream stream,
      final boolean prettyPrint) {
    final ObjectMapper mapper = new ObjectMapper();
    try {
      if (prettyPrint) {
        final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        writer.writeValue(stream, obj);
        return;
      }
      mapper.writeValue(stream, obj);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void toJSON(final Object obj, final File file) throws IOException {
    toJSON(obj, file, false);
  }

  public static void toJSON(final Object obj, final File file, final boolean prettyPrint)
      throws IOException {
    final BufferedOutputStream stream =
        new BufferedOutputStream(new FileOutputStream(file));
    try {
      toJSON(obj, stream, prettyPrint);
    } finally {
      stream.close();
    }
  }

  public static Object parseJSONFromStringQuiet(final String json) {
    try {
      return parseJSONFromString(json);
    } catch (final IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static Object parseJSONFromString(final String json) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonFactory factory = new JsonFactory();
    final JsonParser parser = factory.createJsonParser(json);
    final JsonNode node = mapper.readTree(parser);

    return toObjectFromJSONNode(node);
  }

  public static Object parseJSONFromFile(final File file) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonFactory factory = new JsonFactory();
    final JsonParser parser = factory.createJsonParser(file);
    final JsonNode node = mapper.readTree(parser);

    return toObjectFromJSONNode(node);
  }

  public static Object parseJSONFromReader(final Reader reader) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonFactory factory = new JsonFactory();
    final JsonParser parser = factory.createJsonParser(reader);
    final JsonNode node = mapper.readTree(parser);

    return toObjectFromJSONNode(node);
  }

  private static Object toObjectFromJSONNode(final JsonNode node) {
    if (node.isObject()) {
      final HashMap<String, Object> obj = new HashMap<>();
      final Iterator<String> iter = node.getFieldNames();
      while (iter.hasNext()) {
        final String fieldName = iter.next();
        final JsonNode subNode = node.get(fieldName);
        final Object subObj = toObjectFromJSONNode(subNode);
        obj.put(fieldName, subObj);
      }

      return obj;
    } else if (node.isArray()) {
      final ArrayList<Object> array = new ArrayList<>();
      final Iterator<JsonNode> iter = node.getElements();
      while (iter.hasNext()) {
        final JsonNode element = iter.next();
        final Object subObject = toObjectFromJSONNode(element);
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

  public static long getLongFromObject(final Object obj) {
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
  public static void writePropsNoJarDependency(final Map<String, String> properties,
      final Writer writer) throws IOException {
    writer.write("{\n");
    int size = properties.size();

    for (final Map.Entry<String, String> entry : properties.entrySet()) {
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

  private static String quoteAndClean(final String str) {
    if (str == null || str.isEmpty()) {
      return "\"\"";
    }

    final StringBuffer buffer = new StringBuffer(str.length());
    buffer.append('"');
    for (int i = 0; i < str.length(); ++i) {
      final char ch = str.charAt(i);

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
            final String hexCode = Integer.toHexString(ch);
            final int lengthHexCode = hexCode.length();
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

  private static boolean isCharSpecialUnicode(final char ch) {
    if (ch < ' ') {
      return true;
    } else if (ch >= '\u0080' && ch < '\u00a0') {
      return true;
    } else if (ch >= '\u2000' && ch < '\u2100') {
      return true;
    }

    return false;
  }

  /**
   * Reads json file from the classpath placed in the resources folder and returns as string
   *
   * @param filePath
   * @return String json as string
   */
  public static String readJsonFileAsString(final String filePath) {
    InputStream is = null;
    try {
      is = JSONUtils.class.getClassLoader().getResourceAsStream(filePath);
      return IOUtils.toString(is, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      log.error("Exception while reading input file.", e);
      throw new RuntimeException("Exception while reading input file : " + filePath);
    } catch (final Exception ex) {
      log.error("Exception while reading input file.", ex);
      throw new RuntimeException("Exception while reading input file : " + filePath);
    } finally {
      if(is!=null) {
        IOUtils.closeQuietly(is);
      }
    }
  }

  /**
   * Reads json string and returns JsonNode.
   *
   * @param json
   * @return JsonNode
   * @throws IOException
   */
  public static JsonNode readJsonString(final String json) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonFactory factory = new JsonFactory();
    final JsonParser parser = factory.createJsonParser(json);
    final JsonNode node = mapper.readTree(parser);
    return node;
  }

  /**
   * Extract text field value from JsonNode.
   *
   * @param jsonNode
   * @param key
   * @return String
   * @throws IOException
   */
  public static String extractTextFieldValueFromJsonNode(final JsonNode jsonNode, final String key)
      throws IOException {
    return jsonNode.get(key).asText();
  }

  /**
   * Extract text field value from given json string.
   *
   * @param json
   * @param key
   * @return String
   * @throws IOException
   */
  public static String extractTextFieldValueFromJsonString(final String json, final String key)
      throws IOException {
    return extractTextFieldValueFromJsonNode(readJsonString(json), key);
  }
}
