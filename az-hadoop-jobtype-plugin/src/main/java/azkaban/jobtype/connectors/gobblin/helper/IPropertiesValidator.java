/*
 * Copyright 2014-2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype.connectors.gobblin.helper;

import azkaban.utils.Props;


/**
 * Interface of Gobblin properties validator
 */
public interface IPropertiesValidator {

  /**
   * Validates props.
   * @param props
   * @throws UndefinedPropertyException if required property is missing
   * @throws IllegalArgumentException if property is set incorrectly
   */
  public void validate(Props props);
}
