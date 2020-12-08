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
package azkaban.imagemgmt.daos;

import azkaban.imagemgmt.exeception.ImageMgmtException;
import azkaban.imagemgmt.models.BaseModel;
import java.util.Optional;

/**
 * Base interface of the DAOs. This interface exposes some of the common methods.
 * @param <MODEL>
 * @param <ID>
 */
public interface BaseDao<MODEL extends BaseModel, ID extends Object> {

  /**
   * Finds model based on the model id.
   * @param id
   * @return Optional<MODEL>
   * @throws ImageMgmtException
   */
  public Optional<MODEL> findById(ID id) throws ImageMgmtException;
}
