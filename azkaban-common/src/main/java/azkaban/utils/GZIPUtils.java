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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;

public class GZIPUtils {

  public static byte[] gzipString(String str, String encType)
      throws IOException {
    byte[] stringData = str.getBytes(encType);

    return gzipBytes(stringData);
  }

  public static byte[] gzipBytes(byte[] bytes) throws IOException {
    return gzipBytes(bytes, 0, bytes.length);
  }

  public static byte[] gzipBytes(byte[] bytes, int offset, int length)
      throws IOException {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    GZIPOutputStream gzipStream = null;

    gzipStream = new GZIPOutputStream(byteOutputStream);

    gzipStream.write(bytes, offset, length);
    gzipStream.close();
    return byteOutputStream.toByteArray();
  }

  public static byte[] unGzipBytes(byte[] bytes) throws IOException {
    ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);
    GZIPInputStream gzipInputStream = new GZIPInputStream(byteInputStream);

    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    IOUtils.copy(gzipInputStream, byteOutputStream);

    return byteOutputStream.toByteArray();
  }

  public static String unGzipString(byte[] bytes, String encType)
      throws IOException {
    byte[] response = unGzipBytes(bytes);
    return new String(response, encType);
  }
}
