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
package azkaban.imagemgmt.converters;

import azkaban.imagemgmt.dto.BaseDTO;
import azkaban.imagemgmt.models.BaseModel;
import java.util.List;

/**
 * This interface provides API to convert API DTO to DATA_MODEL and vice-versa.
 *
 * @param <API_REQUEST_DTO>  This represents API request DTO to transfer API input request data.
 *                           This can be termed as API specific input schema.
 * @param <API_RESPONSE_DTO> This represents API response DTO to transfer API specific response.
 *                           This can be termed as API specific response schema.
 * @param <DATA_MODEL>       This represents the backend data model and maps to the corresponding
 *                           API specific data model(DTO). Predominantly used DAO layer for
 *                           performing all the crud operations.
 */
public interface Converter<API_REQUEST_DTO extends BaseDTO, API_RESPONSE_DTO extends BaseDTO,
    DATA_MODEL extends BaseModel> {

  /**
   * Converts from API specific request data model (DTO) to backend data model.
   *
   * @param requestDto
   * @return DATA_MODEL
   */
  public DATA_MODEL convertToDataModel(API_REQUEST_DTO requestDto);

  /**
   * Converts from API specific request data models (DTOs) to backend data models.
   *
   * @param requestDtos
   * @return DATA_MODEL
   */
  public List<DATA_MODEL> convertToDataModels(List<API_REQUEST_DTO> requestDtos);

  /**
   * Converts from backend data model to API specific response data model (DTO).
   *
   * @param dataModel
   * @return API_DTO
   */
  public API_RESPONSE_DTO convertToApiResponseDTO(DATA_MODEL dataModel);

  /**
   * Converts from backed data models to API specific response data models (DTOs).
   *
   * @param dataModels
   * @return API_DTO
   */
  public List<API_RESPONSE_DTO> convertToApiResponseDTOs(List<DATA_MODEL> dataModels);

}