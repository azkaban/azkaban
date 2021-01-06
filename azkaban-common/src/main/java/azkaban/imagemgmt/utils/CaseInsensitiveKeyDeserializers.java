/*
 * Copyright 2020 LinkedIn Corp.
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

package azkaban.imagemgmt.utils;

import java.io.IOException;
import org.codehaus.jackson.map.BeanDescription;
import org.codehaus.jackson.map.BeanProperty;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.KeyDeserializer;
import org.codehaus.jackson.map.KeyDeserializers;
import org.codehaus.jackson.type.JavaType;

/**
 * This KeyDeserializers implementation class handles case insensitive deserialization of the keys.
 * This implementation only considers key of type either string or object.
 */
public class CaseInsensitiveKeyDeserializers implements KeyDeserializers {

  public static final CaseInsensitiveKeyDeserializer DESERIALIZER = new CaseInsensitiveKeyDeserializer();

  @Override
  public KeyDeserializer findKeyDeserializer(final JavaType type, final DeserializationConfig config,
      final BeanDescription beanDesc, final BeanProperty property)
      throws JsonMappingException {
    if ((type.getRawClass() != String.class) && (type.getRawClass() != Object.class)) {
      throw new IllegalArgumentException(
          "expected String or Object, found " + type.getRawClass().getName());
    }
    return DESERIALIZER;
  }

  /**
   * KeyDeserializer implementation class to deserialize key to lowercase.
   */
  private static class CaseInsensitiveKeyDeserializer
      extends KeyDeserializer {

    @Override
    public Object deserializeKey(final String key, final DeserializationContext ctxt)
        throws IOException {
      return key.toLowerCase();
    }
  }
}
