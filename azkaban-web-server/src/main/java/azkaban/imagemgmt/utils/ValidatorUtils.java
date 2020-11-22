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

import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This utility class performs the REST API user input validation on the models
 */
public class ValidatorUtils {

  private static final Logger log = LoggerFactory.getLogger(ValidatorUtils.class);
  private static ValidatorFactory validatorFactory;

  private ValidatorUtils() {
  }

  static {
    validatorFactory = Validation.byDefaultProvider()
        .configure()
        .messageInterpolator(new ParameterMessageInterpolator())
        .buildValidatorFactory();
  }

  /**
   * Performs validation the supplied object and creates set of violations. Returns if there is no
   * violation. Returns false if there exist any violation.
   *
   * @param obj
   * @param <T>
   * @return boolean
   */
  public static <T> boolean validateObject(T obj) {
    Validator validator = validatorFactory.getValidator();
    Set<ConstraintViolation<T>> violations = validator.validate(obj);
    if (violations.isEmpty()) {
      return true;
    }
    log.error("Object validation failed for: " + obj.toString());
    violations.forEach(violation -> log
        .error(violation.getPropertyPath().toString() + " " + violation.getMessage()));
    return false;
  }
}
