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

import azkaban.database.EncodingType;
import azkaban.db.EncodingType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.io.IOUtils;

public class GZIPUtils {

  public static byte[] gzipString(final String str, final String encType)
          throws IOException {
    final byte[] stringData = str.getBytes(encType);

    return gzipBytes(stringData);
  }

  public static byte[] gzipBytes(final byte[] bytes) throws IOException {
    return gzipBytes(bytes, 0, bytes.length);
  }

  public static byte[] gzipBytes(final byte[] bytes, final int offset, final int length)
          throws IOException {
    final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    GZIPOutputStream gzipStream = null;

    gzipStream = new GZIPOutputStream(byteOutputStream);

    gzipStream.write(bytes, offset, length);
    gzipStream.close();
    return byteOutputStream.toByteArray();
  }

  public static byte[] unGzipBytes(final byte[] bytes) throws IOException {
    final ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);
    final GZIPInputStream gzipInputStream = new GZIPInputStream(byteInputStream);

    final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    IOUtils.copy(gzipInputStream, byteOutputStream);
    gzipInputStream.close();
    return byteOutputStream.toByteArray();
  }

  public static String unGzipString(final byte[] bytes, final String encType)
          throws IOException {
    final byte[] response = unGzipBytes(bytes);
    return new String(response, encType);
  }

  public static Object transformBytesToObject(final byte[] data, final EncodingType encType)
          throws IOException {
    if (encType == EncodingType.GZIP) {
      final String jsonString = GZIPUtils.unGzipString(data, "UTF-8");
      return JSONUtils.parseJSONFromString(jsonString);
    } else {
      final String jsonString = new String(data, "UTF-8");
      return JSONUtils.parseJSONFromString(jsonString);
    }
  }

}

