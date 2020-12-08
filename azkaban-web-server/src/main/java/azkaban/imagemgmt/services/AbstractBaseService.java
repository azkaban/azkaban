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
package azkaban.imagemgmt.services;

import azkaban.imagemgmt.cache.BaseCache;
import azkaban.imagemgmt.daos.BaseDao;
import azkaban.imagemgmt.models.BaseModel;
import java.util.Optional;

/**
 * Base abstract class for the service layer. This class is primarily involved in performing common
 * functionalities such as populating the cache if there is a change in the underlying model.
 *
 * @param <MODEL> model representing the data transfer
 * @param <ID>    id of the model
 * @param <KEY>   key of the cache
 */
public abstract class AbstractBaseService<MODEL extends BaseModel, ID extends Object,
    KEY extends Object> {

  private final BaseDao<MODEL, ID> baseDao;
  private final BaseCache<KEY, MODEL> baseCache;

  public AbstractBaseService(final BaseDao<MODEL, ID> baseDao,
      final BaseCache<KEY, MODEL> baseCache) {
    this.baseDao = baseDao;
    this.baseCache = baseCache;
  }

  /**
   * Notify the change in the underlying model so that the changes are propagated to the underlying
   * cache (if used).
   *
   * @param id
   * @param key
   * @param notifyAction
   */
  protected void notifyUpdate(final ID id, final KEY key, final NotifyAction notifyAction) {
    switch (notifyAction) {
      case ADD:
        final Optional<MODEL> optionalModel = baseDao.findById(id);
        if (optionalModel.isPresent()) {
          baseCache.add(key, optionalModel.get());
        }
        break;
      case REMOVE:
        baseCache.remove(key);
        break;
      default:
    }
  }

  /**
   * Action to be performed while notifying update.
   */
  protected enum NotifyAction {
    ADD,
    REMOVE
  }
}
