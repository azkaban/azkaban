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

import java.util.List;
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
  private static final ValidatorFactory validatorFactory;

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
   * @param validationErrors
   * @return boolean
   */
  public static <T> boolean validateObject(final T obj, final List<String> validationErrors) {
    final Validator validator = validatorFactory.getValidator();
    final Set<ConstraintViolation<T>> violations = validator.validate(obj);
    if (violations.isEmpty()) {
      return true;
    }
    log.error("Object validation failed for: " + obj.toString());
    violations.forEach(violation -> {
      validationErrors.add(violation.getMessage());
      log.error(violation.getPropertyPath().toString() + " " + violation.getMessage());
    });
    return false;
  }

  /**
   * Performs validation the supplied object and creates set of violations. Returns if there is no
   * violation. Returns false if there exist any violation. This method performs validation for the
   * annotations with validation group class.
   *
   * @param obj
   * @param <T>
   * @return boolean
   */
  public static <T> boolean validateObject(final T obj, final List<String> validationErrors,
      final Class<?> validationGroup) {
    final Validator validator = validatorFactory.getValidator();
    final Set<ConstraintViolation<T>> violations = validator.validate(obj, validationGroup);
    if (violations.isEmpty()) {
      return true;
    }
    log.error("Object validation failed for: " + obj.toString());
    violations.forEach(violation -> {
      validationErrors.add(violation.getMessage());
      log.error(violation.getPropertyPath().toString() + " " + violation.getMessage());
    });
    return false;
  }
}
