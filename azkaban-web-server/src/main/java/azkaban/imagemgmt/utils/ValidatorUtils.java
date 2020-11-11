package azkaban.imagemgmt.utils;

import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static <T> boolean validateObject(T obj) {
    Validator validator = validatorFactory.getValidator();
    Set<ConstraintViolation<T>> violations = validator.validate(obj);
    if (violations.isEmpty()) {
      return true;
    }
    log.error("Object validation failed for: " + obj.toString());
    violations.forEach(violation -> log.error(violation.getPropertyPath().toString() + " " + violation.getMessage()));
    return false;
  }
}
