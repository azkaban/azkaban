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

import azkaban.imagemgmt.dto.BaseDTO;
import azkaban.imagemgmt.exception.ErrorCode;
import azkaban.imagemgmt.exception.ImageMgmtInvalidInputException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This utility class is responsible for converting input JSON string to API model.
 */
@Singleton
public class ConverterUtils {

  private static final Logger log = LoggerFactory.getLogger(ConverterUtils.class);

  private final ObjectMapper objectMapper;

  @Inject
  public ConverterUtils(final ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /**
   * Converts Json input payload to API specific model (DTO).
   *
   * @param jsonPayloadString
   * @param dtoClass
   * @param <T>
   * @return DTO
   * @throws ImageMgmtInvalidInputException
   */
  public <T extends BaseDTO> T convertToDTO(final String jsonPayloadString, final Class<T> dtoClass)
      throws ImageMgmtInvalidInputException {
    try {
      return this.objectMapper.readValue(jsonPayloadString, dtoClass);
    } catch (final JsonParseException e) {
      log.error("Exception while parsing input json ", e);
      throw new ImageMgmtInvalidInputException(ErrorCode.BAD_REQUEST,
          "Exception while reading input payload. Invalid input.");
    } catch (final JsonMappingException e) {
      log.error("Exception while converting input json ", e);
      throw new ImageMgmtInvalidInputException(ErrorCode.BAD_REQUEST,
          "Exception while reading input payload. Invalid input.");
    } catch (final IOException e) {
      log.error("IOException occurred while converting input json ", e);
      throw new ImageMgmtInvalidInputException(ErrorCode.BAD_REQUEST,
          "Exception while reading input payload. Invalid input.");
    }
  }

  /**
   * Converts input json payload to API speific models (DTOs).
   *
   * @param jsonPayloadString
   * @param <T>
   * @return List<T>
   * @throws ImageMgmtInvalidInputException
   */
  public <T extends BaseDTO> List<T> convertToDTOs(final String jsonPayloadString,
      final Class<T> dtoClass)
      throws ImageMgmtInvalidInputException {
    try {
      final TypeFactory typeFactory = this.objectMapper.getTypeFactory();
      final JavaType javaType = typeFactory.constructParametricType(ArrayList.class, dtoClass);
      return this.objectMapper.readValue(jsonPayloadString, javaType);
    } catch (final JsonParseException e) {
      log.error("Exception while parsing input json ", e);
      throw new ImageMgmtInvalidInputException(
          "Exception while reading input payload. Invalid input.");
    } catch (final JsonMappingException e) {
      log.error("Exception while converting input json ", e);
      throw new ImageMgmtInvalidInputException(
          "Exception while reading input payload. Invalid input.");
    } catch (final IOException e) {
      log.error("IOException occurred while converting input json ", e);
      throw new ImageMgmtInvalidInputException(
          "Exception while reading input payload. Invalid input.");
    }
  }
}
